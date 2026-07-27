/*
lazedo: reference-counting for buckets shared by multiple BucketClaims.

A Bucket is cluster-scoped; when several BucketClaims (e.g. same-named claims in
different namespaces) resolve to the same Bucket, deleting one must NOT delete
the shared bucket while others still use it. COSI v1alpha1 has no such semantics
and native ownerReferences can't span the namespaced-claim → cluster-scoped-Bucket
boundary, so we track a SET of claim UIDs on the Bucket (idempotent under
re-reconcile, unlike a raw counter) and only delete the Bucket object when the
set empties. The Bucket's DeletionPolicy is then applied downstream by the
sidecar's bucket controller (Delete → empty+remove; Retain → keep).
*/
package bucketclaim

import (
	"context"
	"encoding/json"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

const claimRefsAnnotation = "cosi.lazedo.dev/claim-refs"

func getRefs(bucket *v1alpha1.Bucket) []string {
	raw := bucket.Annotations[claimRefsAnnotation]
	if raw == "" {
		return nil
	}
	var refs []string
	_ = json.Unmarshal([]byte(raw), &refs)
	return refs
}

func setRefs(bucket *v1alpha1.Bucket, refs []string) {
	if bucket.Annotations == nil {
		bucket.Annotations = map[string]string{}
	}
	b, _ := json.Marshal(refs)
	bucket.Annotations[claimRefsAnnotation] = string(b)
}

// addClaimRef records that a BucketClaim uses this Bucket (idempotent), with
// optimistic-locking retry against concurrent claim reconciles.
func (b *BucketClaimListener) addClaimRef(ctx context.Context, bucketName string, uid types.UID) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucket, err := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		for _, r := range getRefs(bucket) {
			if r == string(uid) {
				return nil // already present
			}
		}
		setRefs(bucket, append(getRefs(bucket), string(uid)))
		_, err = b.buckets().Update(ctx, bucket, metav1.UpdateOptions{})
		return err
	})
}

// removeClaimRef drops a BucketClaim's reference; empty=true when it was the last
// one (the caller should then delete the Bucket object). A missing Bucket counts
// as already gone (empty=false, no-op).
func (b *BucketClaimListener) removeClaimRef(ctx context.Context, bucketName string, uid types.UID) (empty bool, err error) {
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		bucket, gerr := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if kubeerrors.IsNotFound(gerr) {
			empty = false
			return nil
		}
		if gerr != nil {
			return gerr
		}
		var kept []string
		for _, r := range getRefs(bucket) {
			if r != string(uid) {
				kept = append(kept, r)
			}
		}
		setRefs(bucket, kept)
		if _, uerr := b.buckets().Update(ctx, bucket, metav1.UpdateOptions{}); uerr != nil {
			return uerr
		}
		empty = len(kept) == 0
		return nil
	})
	return empty, err
}
