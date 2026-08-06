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

package bucket

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	buckets "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
	bucketapi "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/typed/objectstorage/v1alpha1"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// BucketListener manages Bucket objects
type BucketListener struct {
	provisionerClient cosi.ProvisionerClient
	driverName        string

	eventRecorder record.EventRecorder

	kubeClient   kube.Interface
	bucketClient buckets.Interface
}

// NewBucketListener returns a resource handler for Bucket objects
func NewBucketListener(driverName string, client cosi.ProvisionerClient) *BucketListener {
	bl := &BucketListener{
		driverName:        driverName,
		provisionerClient: client,
	}

	return bl
}

// Add attempts to create a bucket for a given bucket. This function must be idempotent
//
// Return values
//   - nil - Bucket successfully provisioned
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *BucketListener) Add(ctx context.Context, inputBucket *v1alpha1.Bucket) error {
	bucket := inputBucket.DeepCopy()

	var err error

	if !bucket.GetDeletionTimestamp().IsZero() {
		return b.handleDeletion(ctx, bucket)
	}

	klog.V(3).InfoS("Add Bucket",
		"name", bucket.ObjectMeta.Name)

	if bucket.Spec.BucketClassName == "" {
		return b.recordError(inputBucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, fmt.Errorf("%w for Bucket %v", consts.ErrUndefinedBucketClassName, bucket.Name))
	}

	if !strings.EqualFold(bucket.Spec.DriverName, b.driverName) {
		klog.V(5).InfoS("Skipping bucket for driver",
			"bucket", bucket.ObjectMeta.Name,
			"driver", bucket.Spec.DriverName,
		)
		return nil
	}

	if bucket.Status.BucketReady {
		klog.V(5).InfoS("BucketExists",
			"bucket", bucket.ObjectMeta.Name,
			"driver", bucket.Spec.DriverName,
		)

		return nil
	}

	bucketReady := false
	var bucketID string

	if bucket.Spec.ExistingBucketID != "" {
		bucketReady = true
		bucketID = bucket.Spec.ExistingBucketID
		if bucket.Spec.Parameters == nil {
			bucketClass, err := b.bucketClasses().Get(ctx, bucket.Spec.BucketClassName, metav1.GetOptions{})
			if kubeerrors.IsNotFound(err) {
				return b.recordError(inputBucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
			} else if err != nil {
				klog.V(3).ErrorS(err, "Error fetching bucketClass",
					"bucketClass", bucket.Spec.BucketClassName,
					"bucket", bucket.ObjectMeta.Name)
				return b.recordError(inputBucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
			}

			if bucketClass.Parameters != nil {
				param := make(map[string]string)
				for k, v := range bucketClass.Parameters {
					param[k] = v
				}

				bucket.Spec.Parameters = param
			}
		}
	} else {
		// lazedo: expose the originating BucketClaim's namespace to the driver
		// so multi-tenancy drivers can route (e.g. per-subscription minio
		// tenant) or scope naming. Copied map — never mutate the shared spec.
		params := make(map[string]string, len(bucket.Spec.Parameters)+2)
		for k, v := range bucket.Spec.Parameters {
			params[k] = v
		}
		if bucket.Spec.BucketClaim != nil {
			params["cosi.lazedo.dev/claim-namespace"] = bucket.Spec.BucketClaim.Namespace
			params["cosi.lazedo.dev/claim-name"] = bucket.Spec.BucketClaim.Name
		}
		req := &cosi.DriverCreateBucketRequest{
			Parameters: params,
			Name:       bucket.ObjectMeta.Name,
		}

		rsp, err := b.provisionerClient.DriverCreateBucket(ctx, req)
		if err != nil {
			if status.Code(err) != codes.AlreadyExists {
				return b.recordError(inputBucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, fmt.Errorf("failed to create bucket: %w", err))
			}
		}

		if rsp == nil {
			err = consts.ErrInternal
			klog.V(3).ErrorS(err, "Internal Error from driver",
				"bucket", bucket.ObjectMeta.Name)
			return fmt.Errorf("%w for Bucket %v", err, bucket.Name)
		}

		if rsp.BucketId != "" {
			bucketID = rsp.BucketId
			bucketReady = true
		} else {
			klog.V(3).ErrorS(err, "DriverCreateBucket returned an empty bucketID",
				"bucket", bucket.ObjectMeta.Name)
			return fmt.Errorf("%w for Bucket %v", consts.ErrEmptyBucketID, bucket.Name)
		}

		// Now we update the BucketReady status of BucketClaim
		if bucket.Spec.BucketClaim != nil {
			ref := bucket.Spec.BucketClaim
			if err := b.updateClaimStatusWithRetry(ctx, ref.Namespace, ref.Name, func(cur *v1alpha1.BucketClaim) {
				cur.Status.BucketReady = true
			}); err != nil {
				klog.V(3).ErrorS(err, "Failed to update bucketClaim",
					"bucketClaim", ref.Name,
					"bucket", bucket.ObjectMeta.Name)
				return b.recordError(bucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
			}

			klog.V(5).Infof("Successfully updated status of bucketClaim: %s, bucket: %s", ref.Name, bucket.ObjectMeta.Name)
		}
	}

	// lazedo: the central controller annotates the Bucket (claim refcount)
	// right after creation, so an Update built on the event copy hits a
	// Conflict and the workqueue retries with the SAME stale object -- seen
	// live wedging the first claim of a fresh install forever. Re-GET and
	// re-apply the mutation on every attempt instead.
	if bucket, err = b.updateBucketWithRetry(ctx, bucket.ObjectMeta.Name, func(cur *v1alpha1.Bucket) {
		controllerutil.AddFinalizer(cur, consts.BucketFinalizer)
	}); err != nil {
		klog.V(3).ErrorS(err, "Failed to update bucket finalizers", "bucket", inputBucket.ObjectMeta.Name)
		return b.recordError(inputBucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket,
			fmt.Errorf("failed to update bucket finalizers: %w", err))
	}

	klog.V(5).Infof("Successfully added finalizer to bucket: %s", bucket.ObjectMeta.Name)

	// if this step fails, then controller will retry with backoff
	if _, err = b.updateBucketStatusWithRetry(ctx, bucket.ObjectMeta.Name, func(cur *v1alpha1.Bucket) {
		cur.Status.BucketReady = bucketReady
		cur.Status.BucketID = bucketID
	}); err != nil {
		klog.V(3).ErrorS(err, "Failed to update bucket status",
			"bucket", bucket.ObjectMeta.Name)
		return b.recordError(bucket, v1.EventTypeWarning, v1alpha1.FailedCreateBucket,
			fmt.Errorf("failed to update bucket status: %w", err))
	}

	klog.V(3).InfoS("Add Bucket success",
		"bucket", bucket.ObjectMeta.Name,
		"bucketID", bucketID,
		"ns", bucket.ObjectMeta.Namespace)

	return nil
}

// Update attempts to reconcile changes to a given bucket. This function must be idempotent
// Return values
//   - nil - Bucket successfully reconciled
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *BucketListener) Update(ctx context.Context, old, new *v1alpha1.Bucket) error {
	klog.V(3).InfoS("Update Bucket",
		"name", old.Name)

	bucket := new.DeepCopy()

	var err error

	if !bucket.GetDeletionTimestamp().IsZero() {
		return b.handleDeletion(ctx, bucket)
	}

	if err = b.Add(ctx, bucket); err != nil {
		return err
	}

	klog.V(3).InfoS("Update Bucket success",
		"name", bucket.ObjectMeta.Name,
		"ns", bucket.ObjectMeta.Namespace)
	return nil
}

func (b *BucketListener) handleDeletion(ctx context.Context, bucket *v1alpha1.Bucket) error {
	var err error

	if controllerutil.ContainsFinalizer(bucket, consts.BABucketFinalizer) {
		bucketClaimNs := bucket.Spec.BucketClaim.Namespace
		bucketClaimName := bucket.Spec.BucketClaim.Name

		bucketAccessList, err := b.bucketAccesses(bucketClaimNs).List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.V(3).ErrorS(err, "Error fetching BucketAccessList",
				"bucket", bucket.ObjectMeta.Name)
			return err
		}

		for _, ba := range bucketAccessList.Items {
			if strings.EqualFold(ba.Spec.BucketClaimName, bucketClaimName) {
				if err = b.bucketAccesses(bucketClaimNs).Delete(ctx, ba.Name, metav1.DeleteOptions{}); err != nil {
					klog.V(3).ErrorS(err, "Error deleting BucketAccess",
						"name", ba.Name,
						"bucket", bucket.ObjectMeta.Name)
					return err
				}
			}
		}

		controllerutil.RemoveFinalizer(bucket, consts.BABucketFinalizer)
	}

	if controllerutil.ContainsFinalizer(bucket, consts.BucketFinalizer) {
		if err = b.deleteBucketOp(ctx, bucket); err != nil {
			return b.recordError(bucket, v1.EventTypeWarning, v1alpha1.FailedDeleteBucket, err)
		}
		controllerutil.RemoveFinalizer(bucket, consts.BucketFinalizer)
	}

	// Mirror the removals decided above onto a FRESH copy -- writing the
	// event copy back loses concurrent writes and conflicts forever.
	if _, err = b.updateBucketWithRetry(ctx, bucket.ObjectMeta.Name, func(cur *v1alpha1.Bucket) {
		if !controllerutil.ContainsFinalizer(bucket, consts.BABucketFinalizer) {
			controllerutil.RemoveFinalizer(cur, consts.BABucketFinalizer)
		}
		if !controllerutil.ContainsFinalizer(bucket, consts.BucketFinalizer) {
			controllerutil.RemoveFinalizer(cur, consts.BucketFinalizer)
		}
	}); err != nil {
		klog.V(3).ErrorS(err, "Error updating bucket after removing finalizers",
			"bucket", bucket.ObjectMeta.Name)
		return err
	}

	return nil
}

// Delete attempts to delete a bucket. This function must be idempotent
// Delete function is called when the bucket was not able to add finalizers while creation.
// Hence we will take care of removing the BucketClaim finalizer before deleting the Bucket object.
// Return values
//   - nil - Bucket successfully deleted
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *BucketListener) Delete(ctx context.Context, inputBucket *v1alpha1.Bucket) error {
	klog.V(3).InfoS("Delete Bucket",
		"name", inputBucket.ObjectMeta.Name,
		"bucketclass", inputBucket.Spec.BucketClassName)

	if inputBucket.Spec.BucketClaim != nil {
		ref := inputBucket.Spec.BucketClaim
		klog.V(3).Infof("Removing finalizer of bucketClaim: %s before deleting bucket: %s", ref.Name, inputBucket.ObjectMeta.Name)

		if err := b.updateClaimWithRetry(ctx, ref.Namespace, ref.Name, func(cur *v1alpha1.BucketClaim) {
			controllerutil.RemoveFinalizer(cur, consts.BCFinalizer)
		}); err != nil {
			klog.V(3).ErrorS(err, "Error removing bucketClaim finalizer",
				"bucket", inputBucket.ObjectMeta.Name,
				"bucketClaim", ref.Name)
			return err
		}
	}

	return nil
}

// InitializeKubeClient initializes the kubernetes client
func (b *BucketListener) InitializeKubeClient(k kube.Interface) {
	b.kubeClient = k
}

// InitializeBucketClient initializes the object storage bucket client
func (b *BucketListener) InitializeBucketClient(bc buckets.Interface) {
	b.bucketClient = bc
}

// InitializeEventRecorder initializes the event recorder
func (b *BucketListener) InitializeEventRecorder(er record.EventRecorder) {
	b.eventRecorder = er
}

func (b *BucketListener) deleteBucketOp(ctx context.Context, bucket *v1alpha1.Bucket) error {
	if !strings.EqualFold(bucket.Spec.DriverName, b.driverName) {
		klog.V(5).InfoS("Skipping bucket for provisioner",
			"bucket", bucket.ObjectMeta.Name,
			"driver", bucket.Spec.DriverName,
		)
		return nil
	}

	// We ask the driver to clean up the bucket from the storage provider
	// only when the retain policy is set to Delete
	if bucket.Spec.DeletionPolicy == v1alpha1.DeletionPolicyDelete {
		req := &cosi.DriverDeleteBucketRequest{
			BucketId:      bucket.Status.BucketID,
			DeleteContext: bucket.Spec.Parameters,
		}

		if _, err := b.provisionerClient.DriverDeleteBucket(ctx, req); err != nil {
			if status.Code(err) != codes.NotFound {
				return fmt.Errorf("failed to delete bucket: %w", err)
			}
		}

		klog.V(5).Infof("Successfully deleted bucketID: %s from the object storage for bucket: %s", bucket.Status.BucketID, bucket.ObjectMeta.Name)
	}

	if bucket.Spec.BucketClaim != nil {
		ref := bucket.Spec.BucketClaim
		if err := b.updateClaimWithRetry(ctx, ref.Namespace, ref.Name, func(cur *v1alpha1.BucketClaim) {
			controllerutil.RemoveFinalizer(cur, consts.BCFinalizer)
		}); err != nil {
			klog.V(3).ErrorS(err, "Error removing finalizer from bucketClaim",
				"bucketClaim", ref.Name,
				"bucket", bucket.ObjectMeta.Name)
			return err
		}

		klog.V(5).Infof("Successfully removed finalizer: %s from bucketClaim: %s for bucket: %s", consts.BCFinalizer, ref.Name, bucket.ObjectMeta.Name)
	}

	return nil
}

// lazedo: conflict-safe writers. Each attempt re-GETs the object and
// re-applies the mutation, so a concurrent writer (the controller's claim
// refcount annotation, another sidecar path) can never wedge a reconcile
// with a perpetually stale copy.
func (b *BucketListener) updateBucketWithRetry(ctx context.Context, name string, mutate func(*v1alpha1.Bucket)) (*v1alpha1.Bucket, error) {
	var out *v1alpha1.Bucket
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := b.buckets().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		out, err = b.buckets().Update(ctx, cur, metav1.UpdateOptions{})
		return err
	})
	return out, err
}

func (b *BucketListener) updateBucketStatusWithRetry(ctx context.Context, name string, mutate func(*v1alpha1.Bucket)) (*v1alpha1.Bucket, error) {
	var out *v1alpha1.Bucket
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := b.buckets().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		out, err = b.buckets().UpdateStatus(ctx, cur, metav1.UpdateOptions{})
		return err
	})
	return out, err
}

func (b *BucketListener) updateClaimWithRetry(ctx context.Context, ns, name string, mutate func(*v1alpha1.BucketClaim)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := b.bucketClaims(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		_, err = b.bucketClaims(ns).Update(ctx, cur, metav1.UpdateOptions{})
		return err
	})
}

func (b *BucketListener) updateClaimStatusWithRetry(ctx context.Context, ns, name string, mutate func(*v1alpha1.BucketClaim)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := b.bucketClaims(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		mutate(cur)
		_, err = b.bucketClaims(ns).UpdateStatus(ctx, cur, metav1.UpdateOptions{})
		return err
	})
}

func (b *BucketListener) buckets() bucketapi.BucketInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (b *BucketListener) bucketClasses() bucketapi.BucketClassInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClasses()
	}
	panic("uninitialized listener")
}

func (b *BucketListener) bucketClaims(namespace string) bucketapi.BucketClaimInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}

	panic("uninitialized listener")
}

func (b *BucketListener) bucketAccesses(namespace string) bucketapi.BucketAccessInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketAccesses(namespace)
	}
	panic("uninitialized listener")
}

// recordError during the processing of the objects
func (b *BucketListener) recordError(subject runtime.Object, eventtype, reason string, err error) error {
	if b.eventRecorder == nil {
		return err
	}
	b.eventRecorder.Event(subject, eventtype, reason, err.Error())

	return err
}

// recordEvent during the processing of the objects
func (b *BucketListener) recordEvent(subject runtime.Object, eventtype, reason, message string, args ...any) {
	if b.eventRecorder == nil {
		return
	}
	b.eventRecorder.Event(subject, eventtype, reason, fmt.Sprintf(message, args...))
}
