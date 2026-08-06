/*
lazedo: reference-counting for buckets shared by multiple BucketClaims.

A Bucket is cluster-scoped; when several BucketClaims (create-or-adopt
sharing, or same-named claims in different namespaces) resolve to the same
Bucket, deleting one must NOT delete the shared bucket while others still use
it. COSI v1alpha1 has no such semantics and native ownerReferences can't span
the namespaced-claim → cluster-scoped-Bucket boundary, so we track a SET of
binder entries on the Bucket (idempotent under re-reconcile, unlike a raw
counter) and only delete the Bucket object when the set empties. The LAST
binder to leave decides the fate of the data: its class's deletionPolicy is
written onto the Bucket in the same update that empties the set, and the
sidecar applies it downstream (Delete → empty+remove; Retain → keep).

The annotation format and mode marker live in the client API package
(binderrefs.go) — they are contract shared with the sidecar.
*/
package bucketclaim

import (
	"context"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

// addClaimRef records that a BucketClaim uses this Bucket (idempotent), with
// optimistic-locking retry against concurrent claim reconciles. The first
// binder also stamps the bucket's binding mode. Returns the binder count
// after the add.
func (b *BucketClaimListener) addClaimRef(ctx context.Context, bucketName string, claim *v1alpha1.BucketClaim) (binders int, err error) {
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucket, err := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		refs := v1alpha1.GetBinderRefs(bucket)
		for _, r := range refs {
			if r.UID == string(claim.UID) {
				binders = len(refs)
				return nil // already present
			}
		}
		if len(refs) == 0 {
			// First binder decides the mode; exclusivity holds over time.
			v1alpha1.SetBindingMode(bucket, claim.Spec.Exclusive)
		}
		refs = append(refs, v1alpha1.BinderRef{
			Namespace: claim.Namespace, Name: claim.Name, UID: string(claim.UID),
		})
		v1alpha1.SetBinderRefs(bucket, refs)
		binders = len(refs)
		_, err = b.buckets().Update(ctx, bucket, metav1.UpdateOptions{})
		return err
	})
	return binders, err
}

// removeClaimRef drops a BucketClaim's reference; empty=true when it was the
// last one (the caller should then delete the Bucket object). When the set
// empties and lastBinderPolicy is non-empty, it is written as the Bucket's
// deletionPolicy in the SAME update — the last binder decides whether the
// data goes (Delete) or stays (Retain). A missing Bucket counts as already
// gone (empty=false, no-op).
func (b *BucketClaimListener) removeClaimRef(ctx context.Context, bucketName string, uid types.UID, lastBinderPolicy v1alpha1.DeletionPolicy) (empty bool, err error) {
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucket, gerr := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if kubeerrors.IsNotFound(gerr) {
			empty = false
			return nil
		}
		if gerr != nil {
			return gerr
		}
		var kept []v1alpha1.BinderRef
		for _, r := range v1alpha1.GetBinderRefs(bucket) {
			if r.UID != string(uid) {
				kept = append(kept, r)
			}
		}
		v1alpha1.SetBinderRefs(bucket, kept)
		if len(kept) == 0 && lastBinderPolicy != "" {
			bucket.Spec.DeletionPolicy = lastBinderPolicy
		}
		if _, uerr := b.buckets().Update(ctx, bucket, metav1.UpdateOptions{}); uerr != nil {
			return uerr
		}
		empty = len(kept) == 0
		return nil
	})
	return empty, err
}
