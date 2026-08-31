/* Copyright 2021 The Kubernetes Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bucketaccess

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kube "k8s.io/client-go/kubernetes"
	kubecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	buckets "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
	bucketapi "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/typed/objectstorage/v1alpha1"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// BucketAccessListener manages Bucket objects
type BucketAccessListener struct {
	provisionerClient cosi.ProvisionerClient
	driverName        string

	eventRecorder record.EventRecorder

	kubeClient   kube.Interface
	bucketClient buckets.Interface
}

// NewBucketAccessListener returns a resource handler for BucketAccess objects
func NewBucketAccessListener(driverName string, client cosi.ProvisionerClient) *BucketAccessListener {
	return &BucketAccessListener{
		driverName:        driverName,
		provisionerClient: client,
	}
}

// Add attempts to provision credentials to access a given bucket. This function must be idempotent
//
// Return values
//   - nil - BucketAccess successfully granted
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (bal *BucketAccessListener) Add(ctx context.Context, inputBucketAccess *v1alpha1.BucketAccess) error {
	bucketAccess := inputBucketAccess.DeepCopy()

	if !bucketAccess.GetDeletionTimestamp().IsZero() {
		klog.V(3).InfoS("BucketAccess has deletion timestamp, handling deletion",
			"name", bucketAccess.ObjectMeta.Name)
		return bal.deleteBucketAccessOp(ctx, bucketAccess)
	}

	if bucketAccess.Status.AccessGranted && bucketAccess.Status.AccountID != "" {
		klog.V(3).InfoS("BucketAccess already exists", bucketAccess.ObjectMeta.Name)
		return nil
	}

	bucketClaimName := bucketAccess.Spec.BucketClaimName
	klog.V(3).InfoS("Add BucketAccess",
		"name", bucketAccess.ObjectMeta.Name,
		"bucketClaim", bucketClaimName,
	)

	bucketAccessClassName := bucketAccess.Spec.BucketAccessClassName
	klog.V(3).InfoS("Add BucketAccess",
		"name", bucketAccess.ObjectMeta.Name,
		"BucketAccessClassName", bucketAccessClassName,
	)

	secretCredName := bucketAccess.Spec.CredentialsSecretName
	if secretCredName == "" {
		return consts.ErrUndefinedSecretName
	}

	bucketAccessClass, err := bal.bucketAccessClasses().Get(ctx, bucketAccessClassName, metav1.GetOptions{})
	if kubeerrors.IsNotFound(err) {
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
	} else if err != nil {
		klog.ErrorS(err, "Failed to fetch bucketAccessClass", "bucketAccessClass", bucketAccessClassName)
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to fetch BucketAccessClass: %w", err))
	}

	if !strings.EqualFold(bucketAccessClass.DriverName, bal.driverName) {
		klog.V(5).InfoS("Skipping bucketaccess for driver",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"driver", bucketAccessClass.DriverName,
		)
		return nil
	}

	namespace := bucketAccess.ObjectMeta.Namespace
	bucketClaim, err := bal.bucketClaims(namespace).Get(ctx, bucketClaimName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to fetch bucketClaim", "bucketClaim", bucketClaimName)
		return fmt.Errorf("failed to fetch bucketClaim: %w", err)
	}

	if bucketClaim.Status.BucketName == "" || bucketClaim.Status.BucketReady != true {
		err := consts.ErrInvalidBucketState
		klog.V(3).ErrorS(err,
			"Invalid arguments",
			"bucketClaim", bucketClaim.Name,
			"bucketAccess", bucketAccess.ObjectMeta.Name,
		)
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.WaitingForBucket,
			fmt.Errorf("invalid bucket state: %w", err))
	}

	authType := cosi.AuthenticationType_UnknownAuthenticationType
	if bucketAccessClass.AuthenticationType == v1alpha1.AuthenticationTypeKey {
		authType = cosi.AuthenticationType_Key
	} else if bucketAccessClass.AuthenticationType == v1alpha1.AuthenticationTypeIAM {
		authType = cosi.AuthenticationType_IAM
	}

	if authType == cosi.AuthenticationType_IAM && bucketAccess.Spec.ServiceAccountName == "" {
		err = consts.ErrUndefinedServiceAccountName
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
	}

	if bucketAccess.Status.AccessGranted == true {
		klog.V(5).InfoS("AccessAlreadyGranted",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"bucketClaim", bucketClaimName,
		)
		return nil
	}

	bucket, err := bal.buckets().Get(ctx, bucketClaim.Status.BucketName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to fetch bucket", "bucket", bucketClaim.Status.BucketName)
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to fetch bucket: %w", err))
	}

	if bucket.Status.BucketReady != true || bucket.Status.BucketID == "" {
		err = fmt.Errorf("%w: (isReady? %t), (ID empty? %t)",
			consts.ErrInvalidBucketState,
			bucket.Status.BucketReady,
			bucket.Status.BucketID == "")
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.WaitingForBucket, err)
	}

	accountName := consts.AccountNamePrefix + string(bucketAccess.UID)

	// lazedo: expose the BucketAccess's namespace to the driver so
	// multi-tenancy drivers can route the grant (and the advertised endpoint)
	// per subscription. Copied map — never mutate the shared class parameters.
	params := make(map[string]string, len(bucketAccessClass.Parameters)+2)
	for k, v := range bucketAccessClass.Parameters {
		params[k] = v
	}
	params["cosi.lazedo.dev/access-namespace"] = bucketAccess.ObjectMeta.Namespace
	params["cosi.lazedo.dev/access-name"] = bucketAccess.ObjectMeta.Name

	req := &cosi.DriverGrantBucketAccessRequest{
		BucketId:           bucket.Status.BucketID,
		Name:               accountName,
		AuthenticationType: authType,
		Parameters:         params,
	}

	// This needs to be idempotent
	rsp, err := bal.provisionerClient.DriverGrantBucketAccess(ctx, req)
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to grant bucket access: %w", err))
		}
	}

	if rsp.AccountId == "" {
		err = consts.ErrUndefinedAccountID
		klog.V(3).ErrorS(err, "BucketAccess", bucketAccess.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
	}

	credentials := rsp.Credentials
	if len(credentials) != 1 {
		err = consts.ErrInvalidCredentials
		klog.V(3).ErrorS(err, "BucketAccess", bucketAccess.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
	}

	bucketInfoName := consts.BucketInfoPrefix + string(bucketAccess.ObjectMeta.UID)

	bucketInfo := cosiapi.BucketInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name: bucketInfoName,
		},
		Spec: cosiapi.BucketInfoSpec{
			BucketName:         bucket.ObjectMeta.Name,
			AuthenticationType: bucketAccessClass.AuthenticationType,
			Protocols:          []v1alpha1.Protocol{bucketAccess.Spec.Protocol},
		},
	}

	var val *cosi.CredentialDetails
	var ok bool

	if val, ok = credentials[consts.S3Key]; ok {
		secretS3 := &cosiapi.SecretS3{
			Endpoint:        val.Secrets[consts.S3Endpoint],
			Region:          val.Secrets[consts.S3Region],
			AccessKeyID:     val.Secrets[consts.S3SecretAccessKeyID],
			AccessSecretKey: val.Secrets[consts.S3SecretAccessSecretKey],
		}
		// the driver may advertise every URI the credential is valid at
		// (comma-separated); surface them for reachability-aware consumers.
		if uris := val.Secrets[consts.S3Uris]; uris != "" {
			for _, u := range strings.Split(uris, ",") {
				if u = strings.TrimSpace(u); u != "" {
					secretS3.Uris = append(secretS3.Uris, u)
				}
			}
		}

		bucketInfo.Spec.S3 = secretS3
	} else if val, ok = credentials[consts.AzureKey]; ok {
		expiryTs := val.Secrets[consts.AzureSecretExpiryTimeStamp]
		expiryTimestamp, _ := time.Parse(consts.DefaultTimeFormat, expiryTs)
		metav1Time := &metav1.Time{Time: expiryTimestamp}
		secretAzure := &cosiapi.SecretAzure{
			AccessToken:     val.Secrets[consts.AzureSecretAccessToken],
			ExpiryTimeStamp: metav1Time,
		}

		bucketInfo.Spec.Azure = secretAzure
	}

	stringData, err := json.Marshal(bucketInfo)
	if err != nil {
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, consts.ErrBucketInfoConversionFailed)
	}

	// UPSERT, never create-only: a retried grant re-mints credentials, and
	// keeping a stale secret silently hands workloads keys that no longer
	// match status.accountID (bitten 2026-08-31: secret carried grant #1's
	// user while the access recorded grant #2's — a future revoke would
	// delete #2 and leave #1 as live orphaned credentials).
	existing, err := bal.secrets(namespace).Get(ctx, secretCredName, metav1.GetOptions{})
	switch {
	case kubeerrors.IsNotFound(err):
		if _, err := bal.secrets(namespace).Create(ctx, &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:       secretCredName,
				Namespace:  namespace,
				Finalizers: []string{consts.SecretFinalizer},
			},
			StringData: map[string]string{
				"BucketInfo": string(stringData),
			},
			Type: v1.SecretTypeOpaque,
		}, metav1.CreateOptions{}); err != nil && !kubeerrors.IsAlreadyExists(err) {
			klog.V(3).ErrorS(err,
				"Failed to create minted secret",
				"bucketAccess", bucketAccess.ObjectMeta.Name,
				"bucket", bucket.ObjectMeta.Name)
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to create minted secret: %w", err))
		}
	case err != nil:
		klog.V(3).ErrorS(err,
			"Failed to fetch secrets",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"bucket", bucket.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to fetch secrets: %w", err))
	case string(existing.Data["BucketInfo"]) != string(stringData):
		cur := existing.DeepCopy()
		if cur.StringData == nil {
			cur.StringData = map[string]string{}
		}
		cur.StringData["BucketInfo"] = string(stringData)
		if _, err := bal.secrets(namespace).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
			klog.V(3).ErrorS(err,
				"Failed to refresh minted secret",
				"bucketAccess", bucketAccess.ObjectMeta.Name,
				"bucket", bucket.ObjectMeta.Name)
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to refresh minted secret: %w", err))
		}
	}

	if !controllerutil.ContainsFinalizer(bucket, consts.BABucketFinalizer) {
		// lazedo: conflict-safe -- see updateBucketWithRetry.
		if _, err = bal.updateBucketWithRetry(ctx, bucket.ObjectMeta.Name, func(cur *v1alpha1.Bucket) {
			controllerutil.AddFinalizer(cur, consts.BABucketFinalizer)
		}); err != nil {
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
		}
	}

	if !controllerutil.ContainsFinalizer(bucketAccess, consts.BAFinalizer) {
		if bucketAccess, err = bal.updateBucketAccessWithRetry(ctx, bucketAccess.ObjectMeta.Namespace, bucketAccess.ObjectMeta.Name, func(cur *v1alpha1.BucketAccess) {
			controllerutil.AddFinalizer(cur, consts.BAFinalizer)
		}); err != nil {
			klog.V(3).ErrorS(err, "Failed to update BucketAccess finalizer",
				"bucketAccess", inputBucketAccess.ObjectMeta.Name,
				"bucket", bucket.ObjectMeta.Name)
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to update finalizer on BucketAccess %s: %w", inputBucketAccess.ObjectMeta.Name, err))
		}
	}

	// if this step fails, then controller will retry with backoff
	if _, err := bal.updateBucketAccessStatusWithRetry(ctx, bucketAccess.ObjectMeta.Namespace, bucketAccess.ObjectMeta.Name, func(cur *v1alpha1.BucketAccess) {
		cur.Status.AccountID = rsp.AccountId
		cur.Status.AccessGranted = true
	}); err != nil {
		klog.V(3).ErrorS(err, "Failed to update BucketAccess Status",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"bucket", bucket.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to update Status on BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
	}

	return nil
}

// Update attempts to reconcile changes to a given bucketAccess. This function must be idempotent
// Return values
//   - nil - BucketAccess successfully reconciled
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (bal *BucketAccessListener) Update(ctx context.Context, old, new *v1alpha1.BucketAccess) error {
	klog.V(3).InfoS("Update BucketAccess",
		"name", old.ObjectMeta.Name)

	bucketAccess := new.DeepCopy()
	if !bucketAccess.GetDeletionTimestamp().IsZero() {
		if err := bal.deleteBucketAccessOp(ctx, bucketAccess); err != nil {
			return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedRevokeAccess, err)
		}
	} else {
		if err := bal.Add(ctx, bucketAccess); err != nil {
			return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
		}
	}

	klog.V(3).InfoS("Update BucketAccess success",
		"name", old.ObjectMeta.Name)
	return nil
}

// Delete attempts to delete a bucketAccess. This function must be idempotent
// Return values
//   - nil - BucketAccess successfully deleted
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (bal *BucketAccessListener) Delete(ctx context.Context, bucketAccess *v1alpha1.BucketAccess) error {
	klog.V(3).InfoS("Delete BucketAccess",
		"name", bucketAccess.ObjectMeta.Name,
		"bucketClaim", bucketAccess.Spec.BucketClaimName,
	)

	return nil
}

func (bal *BucketAccessListener) deleteBucketAccessOp(ctx context.Context, bucketAccess *v1alpha1.BucketAccess) error {
	// Resolve the bucketID for DriverRevokeBucketAccess. The happy path goes
	// through the BucketClaim, but deletion must NEVER wedge on an already
	// -gone dependency (lazedo: an access deleted after its claim used to
	// requeue forever, silently). When the claim is gone the Bucket object
	// may still exist (adopted/Retain) and carries spec.bucketClaim, so
	// recover it by reference; only when nothing can resolve the bucketID is
	// revocation skipped — loudly, because the backend credential then
	// outlives this access.
	ns := bucketAccess.ObjectMeta.Namespace
	bucketClaimName := bucketAccess.Spec.BucketClaimName
	bucketID := ""
	bucketClaim, err := bal.bucketClaims(ns).Get(ctx, bucketClaimName, metav1.GetOptions{})
	switch {
	case err == nil:
		bucket, berr := bal.buckets().Get(ctx, bucketClaim.Status.BucketName, metav1.GetOptions{})
		if berr != nil && !kubeerrors.IsNotFound(berr) {
			klog.V(3).ErrorS(berr, "Failed to fetch bucket", "bucket", bucketClaim.Status.BucketName)
			return fmt.Errorf("failed to fetch bucket: %w", berr)
		}
		if berr == nil {
			bucketID = bucket.Status.BucketID
		}
	case kubeerrors.IsNotFound(err):
		bucketID = bal.bucketIDByClaimRef(ctx, ns, bucketClaimName)
	default:
		klog.V(3).ErrorS(err, "Failed to fetch bucketClaim", "bucketClaim", bucketClaimName)
		return fmt.Errorf("failed to fetch bucketClaim: %w", err)
	}

	if bucketID != "" {
		req := &cosi.DriverRevokeBucketAccessRequest{
			BucketId:  bucketID,
			AccountId: bucketAccess.Status.AccountID,
		}

		// First we revoke the bucketAccess from the driver
		if _, err := bal.provisionerClient.DriverRevokeBucketAccess(ctx, req); err != nil {
			return fmt.Errorf("failed to revoke access: %w", err)
		}
	} else {
		klog.ErrorS(nil, "bucketClaim and bucket already gone; SKIPPING revocation — the backend credential outlives this access",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"bucketClaim", bucketClaimName,
			"accountId", bucketAccess.Status.AccountID)
	}

	credSecretName := bucketAccess.Spec.CredentialsSecretName
	secret, err := bal.secrets(ns).Get(ctx, credSecretName, metav1.GetOptions{})
	if kubeerrors.IsNotFound(err) {
		// secret already gone: nothing left to tear down but our finalizer.
		return bal.removeBAFinalizer(ctx, bucketAccess)
	}
	if err != nil {
		return err
	}

	if controllerutil.ContainsFinalizer(secret, consts.SecretFinalizer) {
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur, err := bal.secrets(secret.ObjectMeta.Namespace).Get(ctx, secret.ObjectMeta.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			controllerutil.RemoveFinalizer(cur, consts.SecretFinalizer)
			_, err = bal.secrets(cur.ObjectMeta.Namespace).Update(ctx, cur, metav1.UpdateOptions{})
			return err
		})
		if err != nil {
			klog.V(3).ErrorS(err, "Error removing finalizer from secret",
				"secret", secret.ObjectMeta.Name,
				"bucketAccess", bucketAccess.ObjectMeta.Name)
			return err
		}

		klog.V(5).Infof("Successfully removed finalizer from secret: %s, bucketAccess: %s", secret.ObjectMeta.Name, bucketAccess.ObjectMeta.Name)
	}

	err = bal.secrets(secret.ObjectMeta.Namespace).Delete(ctx, credSecretName, metav1.DeleteOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Error deleting secret",
			"secret", secret.ObjectMeta.Name,
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"ns", bucketAccess.ObjectMeta.Namespace)
		return nil
	}

	return bal.removeBAFinalizer(ctx, bucketAccess)
}

// removeBAFinalizer releases the access's own finalizer (idempotent).
func (bal *BucketAccessListener) removeBAFinalizer(ctx context.Context, bucketAccess *v1alpha1.BucketAccess) error {
	if !controllerutil.ContainsFinalizer(bucketAccess, consts.BAFinalizer) {
		return nil
	}
	_, err := bal.updateBucketAccessWithRetry(ctx, bucketAccess.ObjectMeta.Namespace, bucketAccess.ObjectMeta.Name, func(cur *v1alpha1.BucketAccess) {
		controllerutil.RemoveFinalizer(cur, consts.BAFinalizer)
	})
	if err != nil {
		klog.V(3).ErrorS(err, "Error removing finalizer from bucketAccess",
			"bucketAccess", bucketAccess.ObjectMeta.Name)
		return err
	}
	klog.V(5).Infof("Successfully removed finalizer from bucketAccess: %s", bucketAccess.ObjectMeta.Name)
	return nil
}

// bucketIDByClaimRef recovers a bucketID when the BucketClaim is already
// deleted: Bucket objects are cluster-scoped and reference their claim in
// spec.bucketClaim, so scan this driver's buckets for the dangling binding.
// "" when no bucket references the claim anymore.
func (bal *BucketAccessListener) bucketIDByClaimRef(ctx context.Context, ns, claimName string) string {
	list, err := bal.buckets().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to list buckets while recovering a deleted claim's bucket",
			"bucketClaim", ns+"/"+claimName)
		return ""
	}
	for i := range list.Items {
		b := &list.Items[i]
		if b.Spec.DriverName != bal.driverName {
			continue
		}
		if ref := b.Spec.BucketClaim; ref != nil && ref.Namespace == ns && ref.Name == claimName {
			return b.Status.BucketID
		}
	}
	return ""
}

// lazedo: conflict-safe writers -- every attempt re-GETs and re-applies the
// mutation, so concurrent writers cannot wedge the reconcile with a stale
// copy (same failure family as the bucket controller's finalizer wedge).
func (bal *BucketAccessListener) updateBucketWithRetry(ctx context.Context, name string, mutate func(*v1alpha1.Bucket)) (*v1alpha1.Bucket, error) {
	var out *v1alpha1.Bucket
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := bal.buckets().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		out, err = bal.buckets().Update(ctx, cur, metav1.UpdateOptions{})
		return err
	})
	return out, err
}

func (bal *BucketAccessListener) updateBucketAccessWithRetry(ctx context.Context, ns, name string, mutate func(*v1alpha1.BucketAccess)) (*v1alpha1.BucketAccess, error) {
	var out *v1alpha1.BucketAccess
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := bal.bucketAccesses(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		out, err = bal.bucketAccesses(ns).Update(ctx, cur, metav1.UpdateOptions{})
		return err
	})
	return out, err
}

func (bal *BucketAccessListener) updateBucketAccessStatusWithRetry(ctx context.Context, ns, name string, mutate func(*v1alpha1.BucketAccess)) (*v1alpha1.BucketAccess, error) {
	var out *v1alpha1.BucketAccess
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := bal.bucketAccesses(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		out, err = bal.bucketAccesses(ns).UpdateStatus(ctx, cur, metav1.UpdateOptions{})
		return err
	})
	return out, err
}

func (bal *BucketAccessListener) secrets(ns string) kubecorev1.SecretInterface {
	if bal.kubeClient != nil {
		return bal.kubeClient.CoreV1().Secrets(ns)
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) bucketAccesses(ns string) bucketapi.BucketAccessInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().BucketAccesses(ns)
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) buckets() bucketapi.BucketInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) bucketClaims(namespace string) bucketapi.BucketClaimInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) bucketAccessClasses() bucketapi.BucketAccessClassInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().BucketAccessClasses()
	}
	panic("uninitialized listener")
}

// InitializeKubeClient initializes the kubernetes client
func (bal *BucketAccessListener) InitializeKubeClient(k kube.Interface) {
	bal.kubeClient = k
}

// InitializeBucketClient initializes the object storage bucket client
func (bal *BucketAccessListener) InitializeBucketClient(bc buckets.Interface) {
	bal.bucketClient = bc
}

// InitializeEventRecorder initializes the event recorder
func (bal *BucketAccessListener) InitializeEventRecorder(er record.EventRecorder) {
	bal.eventRecorder = er
}

// recordError during the processing of the objects
func (b *BucketAccessListener) recordError(subject runtime.Object, eventtype, reason string, err error) error {
	if b.eventRecorder == nil {
		return err
	}
	b.eventRecorder.Event(subject, eventtype, reason, err.Error())

	return err
}

// recordEvent during the processing of the objects
func (bal *BucketAccessListener) recordEvent(subject runtime.Object, eventtype, reason, message string, args ...any) {
	if bal.eventRecorder == nil {
		return
	}
	bal.eventRecorder.Event(subject, eventtype, reason, fmt.Sprintf(message, args...))
}
