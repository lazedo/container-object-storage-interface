package bucketclaim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
	objectstoragev1alpha1 "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/typed/objectstorage/v1alpha1"
	"sigs.k8s.io/container-object-storage-interface/controller/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// BucketClaimListener is a resource handler for bucket requests objects
type BucketClaimListener struct {
	eventRecorder record.EventRecorder

	kubeClient   kubeclientset.Interface
	bucketClient bucketclientset.Interface

	// tenancyNSPrefix: claims in namespaces with this prefix (kube-bind
	// tenancies) get the namespace remainder prefixed onto the bucket name so
	// equal claim names in different tenancies never collide. Empty = off.
	tenancyNSPrefix string
}

func NewBucketClaimListener() *BucketClaimListener {
	return &BucketClaimListener{}
}

// NewTenancyBucketClaimListener returns a listener that prefixes bucket names
// for claims made inside tenancy namespaces (see tenancyNSPrefix).
func NewTenancyBucketClaimListener(tenancyNSPrefix string) *BucketClaimListener {
	return &BucketClaimListener{tenancyNSPrefix: tenancyNSPrefix}
}

// bucketNameFor derives the cluster-wide Bucket (and object-store bucket) name
// for a claim. Outside tenancies it is the claim name (lazedo naming). Inside a
// tenancy namespace it is "<tenancy>-<claim>", capped to S3's 63-char limit
// with a stable hash suffix when truncation is needed.
func (b *BucketClaimListener) bucketNameFor(bucketClaim *v1alpha1.BucketClaim) string {
	name := bucketClaim.ObjectMeta.Name
	if b.tenancyNSPrefix == "" || !strings.HasPrefix(bucketClaim.ObjectMeta.Namespace, b.tenancyNSPrefix) {
		return name
	}
	tenant := strings.TrimPrefix(bucketClaim.ObjectMeta.Namespace, b.tenancyNSPrefix)
	full := tenant + "-" + name
	if len(full) <= 63 {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	return full[:54] + "-" + hex.EncodeToString(sum[:])[:8]
}

// Add creates a bucket in response to a bucketClaim
func (b *BucketClaimListener) Add(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	if !bucketClaim.GetDeletionTimestamp().IsZero() {
		return b.handleDeletion(ctx, bucketClaim)
	}

	klog.V(3).InfoS("Add BucketClaim",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace,
		"bucketClass", bucketClaim.Spec.BucketClassName,
	)

	err := b.provisionBucketClaimOperation(ctx, bucketClaim)
	if err != nil {
		switch err {
		case util.ErrInvalidBucketClass:
			klog.V(3).ErrorS(err,
				"bucketClaim", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
				"bucketClassName", bucketClaim.Spec.BucketClassName)
		case util.ErrBucketAlreadyExists:
			klog.V(3).InfoS("Bucket already exists",
				"bucketClaim", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
			)
			return nil
		default:
			klog.V(3).ErrorS(err,
				"name", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
				"err", err)
		}
		return err
	}

	klog.V(3).InfoS("Add BucketClaim success",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)
	return nil
}

// update processes any updates  made to the bucket request
func (b *BucketClaimListener) Update(ctx context.Context, old, new *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Update BucketClaim",
		"name", old.Name,
		"ns", old.Namespace)

	bucketClaim := new.DeepCopy()

	if !new.GetDeletionTimestamp().IsZero() {
		return b.handleDeletion(ctx, bucketClaim)
	}

	if err := b.Add(ctx, bucketClaim); err != nil {
		return err
	}

	klog.V(3).InfoS("Update BucketClaim success",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)
	return nil
}

// handleDeletion processes the deletion of a bucketClaim.
func (b *BucketClaimListener) handleDeletion(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	if !controllerutil.ContainsFinalizer(bucketClaim, util.BucketClaimFinalizer) {
		return nil
	}

	bucketName := bucketClaim.Status.BucketName
	if bucketName != "" {
		// lazedo: drop this claim's reference; only delete the Bucket object once
		// no claim references it. The Bucket's DeletionPolicy is applied downstream.
		empty, err := b.removeClaimRef(ctx, bucketName, bucketClaim.ObjectMeta.UID)
		if err != nil {
			klog.V(3).ErrorS(err, "Error updating bucket ref-set",
				"bucket", bucketName, "bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(bucketClaim, v1.EventTypeWarning, v1alpha1.FailedDeleteBucket, err)
		}
		if !empty {
			klog.V(5).Infof("bucket %s still referenced by other claims; detaching claim %s only",
				bucketName, bucketClaim.ObjectMeta.Name)
			// The sidecar removes the claim's finalizer when the Bucket object is
			// deleted; since we keep the shared Bucket, we must remove it here or
			// this claim would hang forever.
			return retry.RetryOnConflict(retry.DefaultRetry, func() error {
				latest, gerr := b.bucketClaims(bucketClaim.Namespace).Get(ctx, bucketClaim.Name, metav1.GetOptions{})
				if kubeerrors.IsNotFound(gerr) {
					return nil
				}
				if gerr != nil {
					return gerr
				}
				if controllerutil.RemoveFinalizer(latest, util.BucketClaimFinalizer) {
					_, uerr := b.bucketClaims(latest.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
					return uerr
				}
				return nil
			})
		}
		if err := b.buckets().Delete(ctx, bucketName, metav1.DeleteOptions{}); err != nil && !kubeerrors.IsNotFound(err) {
			klog.V(3).ErrorS(err, "Error deleting bucket",
				"bucket", bucketName,
				"bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(bucketClaim, v1.EventTypeWarning, v1alpha1.FailedDeleteBucket, err)
		}
		klog.V(5).Infof("last reference gone; requested deletion of bucket: %s for bucketClaim: %s",
			bucketName, bucketClaim.ObjectMeta.Name)
	}

	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *BucketClaimListener) Delete(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Delete BucketClaim",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)

	return nil
}

// provisionBucketClaimOperation attempts to provision a bucket for a given bucketClaim.
//
// Return values
//   - nil - BucketClaim successfully processed
//   - ErrInvalidBucketClass - BucketClass does not exist          [requeue'd with exponential backoff]
//   - ErrBucketAlreadyExists - BucketClaim already processed
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *BucketClaimListener) provisionBucketClaimOperation(ctx context.Context, inputBucketClaim *v1alpha1.BucketClaim) error {
	bucketClaim := inputBucketClaim.DeepCopy()
	if bucketClaim.Status.BucketReady {
		return util.ErrBucketAlreadyExists
	}

	var bucketName string
	var err error

	if bucketClaim.Spec.ExistingBucketName != "" {
		bucketName = bucketClaim.Spec.ExistingBucketName
		bucket, err := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if kubeerrors.IsNotFound(err) {
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		} else if err != nil {
			klog.V(3).ErrorS(err, "Get Bucket with ExistingBucketName error", "name", bucketClaim.Spec.ExistingBucketName)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucket.Spec.BucketClaim = &v1.ObjectReference{
			Name:      bucketClaim.ObjectMeta.Name,
			Namespace: bucketClaim.ObjectMeta.Namespace,
			UID:       bucketClaim.ObjectMeta.UID,
		}

		protocolCopy := make([]v1alpha1.Protocol, len(bucketClaim.Spec.Protocols))
		copy(protocolCopy, bucketClaim.Spec.Protocols)

		bucket.Spec.Protocols = protocolCopy
		_, err = b.buckets().Update(ctx, bucket, metav1.UpdateOptions{})
		if err != nil {
			klog.V(3).ErrorS(err, "Error updating existing bucket",
				"bucket", bucket.ObjectMeta.Name,
				"bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucketClaim.Status.BucketName = bucketName
		bucketClaim.Status.BucketReady = true
	} else {
		bucketClassName := bucketClaim.Spec.BucketClassName
		if bucketClassName == "" {
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, util.ErrInvalidBucketClass)
		}

		bucketClass, err := b.bucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
		if kubeerrors.IsNotFound(err) {
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		} else if err != nil {
			klog.V(3).ErrorS(err, "Get Bucketclass Error", "name", bucketClassName)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		// lazedo: name the Bucket (and therefore the real object-store bucket and
		// the advertised BucketInfo name) after the BucketClaim, so users get the
		// name they indicated instead of bucket-<uid>. Buckets are cluster-scoped
		// and the object-store bucket namespace is flat; claims from tenancy
		// namespaces get a per-tenancy prefix (bucketNameFor) so equal claim
		// names in different subscriptions never collide.
		bucketName = b.bucketNameFor(bucketClaim)

		// create bucket
		bucket := &v1alpha1.Bucket{}
		bucket.Name = bucketName
		bucket.Spec.DriverName = bucketClass.DriverName
		bucket.Status.BucketReady = false
		bucket.Spec.BucketClassName = bucketClassName
		bucket.Spec.DeletionPolicy = bucketClass.DeletionPolicy
		bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

		bucket.Spec.BucketClaim = &v1.ObjectReference{
			Name:      bucketClaim.ObjectMeta.Name,
			Namespace: bucketClaim.ObjectMeta.Namespace,
			UID:       bucketClaim.ObjectMeta.UID,
		}

		protocolCopy := make([]v1alpha1.Protocol, len(bucketClaim.Spec.Protocols))
		copy(protocolCopy, bucketClaim.Spec.Protocols)

		bucket.Spec.Protocols = protocolCopy
		bucket, err = b.buckets().Create(ctx, bucket, metav1.CreateOptions{})
		if err != nil && !kubeerrors.IsAlreadyExists(err) {
			klog.V(3).ErrorS(err, "Error creationg bucket",
				"bucket", bucketName,
				"bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucketClaim.Status.BucketName = bucketName
		bucketClaim.Status.BucketReady = false
	}

	// lazedo: record this claim in the Bucket's ref-set so a shared bucket is
	// only deleted when the last referencing claim goes away.
	if err := b.addClaimRef(ctx, bucketName, bucketClaim.ObjectMeta.UID); err != nil {
		return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
	}

	// Update status with retry logic for conflict errors
	// Store the status values we want to set before entering retry loop
	statusBucketName := bucketClaim.Status.BucketName
	statusBucketReady := bucketClaim.Status.BucketReady

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Fetch the latest version of the BucketClaim
		latest, getErr := b.bucketClaims(bucketClaim.Namespace).Get(
			ctx,
			bucketClaim.Name,
			metav1.GetOptions{},
		)
		if getErr != nil {
			return getErr
		}

		// Apply the status changes to the latest version
		latest.Status.BucketName = statusBucketName
		latest.Status.BucketReady = statusBucketReady

		// Try to update the status
		var updateErr error
		bucketClaim, updateErr = b.bucketClaims(bucketClaim.Namespace).UpdateStatus(
			ctx,
			latest,
			metav1.UpdateOptions{},
		)
		return updateErr
	})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to update status of BucketClaim", "name", bucketClaim.ObjectMeta.Name)
		return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
	}

	// Add the finalizers so that bucketClaim is deleted
	// only after the associated bucket is deleted.
	// Update with retry logic for conflict errors
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Fetch the latest version of the BucketClaim
		latest, getErr := b.bucketClaims(bucketClaim.Namespace).Get(
			ctx,
			bucketClaim.Name,
			metav1.GetOptions{},
		)
		if getErr != nil {
			return getErr
		}

		// Add the finalizer to the latest version
		controllerutil.AddFinalizer(latest, util.BucketClaimFinalizer)

		// Try to update
		var updateErr error
		bucketClaim, updateErr = b.bucketClaims(bucketClaim.Namespace).Update(
			ctx,
			latest,
			metav1.UpdateOptions{},
		)
		return updateErr
	})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to add finalizer BucketClaim", "name", bucketClaim.ObjectMeta.Name)
		return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
	}

	klog.V(3).Infof("Finished creating Bucket %v", bucketName)
	return nil
}

// InitializeKubeClient initializes the kubernetes client
func (b *BucketClaimListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

// InitializeBucketClient initializes the object storage bucket client
func (b *BucketClaimListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

// InitializeEventRecorder initializes the event recorder
func (b *BucketClaimListener) InitializeEventRecorder(er record.EventRecorder) {
	b.eventRecorder = er
}

func (b *BucketClaimListener) buckets() objectstoragev1alpha1.BucketInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (b *BucketClaimListener) bucketClasses() objectstoragev1alpha1.BucketClassInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClasses()
	}
	panic("uninitialized listener")
}

func (b *BucketClaimListener) bucketClaims(namespace string) objectstoragev1alpha1.BucketClaimInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}
	panic("uninitialized listener")
}

// recordError during the processing of the objects
func (b *BucketClaimListener) recordError(subject runtime.Object, eventtype, reason string, err error) error {
	if b.eventRecorder == nil {
		return err
	}
	b.eventRecorder.Event(subject, eventtype, reason, err.Error())

	return err
}

// recordEvent during the processing of the objects
func (b *BucketClaimListener) recordEvent(subject runtime.Object, eventtype, reason, message string, args ...any) {
	if b.eventRecorder == nil {
		return
	}
	b.eventRecorder.Event(subject, eventtype, reason, fmt.Sprintf(message, args...))
}
