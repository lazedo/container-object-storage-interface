/*
lazedo: binder bookkeeping for buckets shared by multiple BucketClaims.

A Bucket is cluster-scoped; when several BucketClaims resolve to the same
Bucket (create-or-adopt sharing), the Bucket tracks its binders in an
annotation so that (a) deletion only happens when the last binder leaves and
(b) the sidecar can flip readiness on every bound claim, not just the
creator. Entries are "namespace/name/uid"; legacy entries carrying only the
UID are still honored for matching. A second annotation records the binding
mode chosen by the first binder (shared vs exclusive) so exclusivity holds
over time, not only at bind.

This lives in the API package because it is contract, shared by the central
controller (writer) and the sidecar (reader).
*/
package v1alpha1

import (
	"encoding/json"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

const (
	// BinderRefsAnnotation holds the JSON list of binder entries.
	BinderRefsAnnotation = "cosi.lazedo.dev/claim-refs"

	// BindingModeAnnotation records the mode of the bucket's first binder.
	BindingModeAnnotation = "cosi.lazedo.dev/binding-mode"

	BindingModeShared    = "shared"
	BindingModeExclusive = "exclusive"
)

// BinderRef identifies one bound BucketClaim. Namespace/Name are empty for
// legacy (uid-only) entries.
type BinderRef struct {
	Namespace string
	Name      string
	UID       string
}

// BinderRefFor encodes the annotation entry for a claim.
func BinderRefFor(claim *BucketClaim) string {
	return claim.Namespace + "/" + claim.Name + "/" + string(claim.UID)
}

// ParseBinderRef decodes one annotation entry (legacy uid-only supported).
func ParseBinderRef(entry string) BinderRef {
	parts := strings.SplitN(entry, "/", 3)
	if len(parts) == 3 {
		return BinderRef{Namespace: parts[0], Name: parts[1], UID: parts[2]}
	}
	return BinderRef{UID: entry}
}

// GetBinderRefs decodes the Bucket's binder list.
func GetBinderRefs(bucket *Bucket) []BinderRef {
	raw := bucket.Annotations[BinderRefsAnnotation]
	if raw == "" {
		return nil
	}
	var entries []string
	_ = json.Unmarshal([]byte(raw), &entries)
	refs := make([]BinderRef, 0, len(entries))
	for _, e := range entries {
		refs = append(refs, ParseBinderRef(e))
	}
	return refs
}

// SetBinderRefs encodes the Bucket's binder list.
func SetBinderRefs(bucket *Bucket, refs []BinderRef) {
	if bucket.Annotations == nil {
		bucket.Annotations = map[string]string{}
	}
	entries := make([]string, 0, len(refs))
	for _, r := range refs {
		if r.Namespace == "" && r.Name == "" {
			entries = append(entries, r.UID) // preserve legacy shape
			continue
		}
		entries = append(entries, r.Namespace+"/"+r.Name+"/"+r.UID)
	}
	b, _ := json.Marshal(entries)
	bucket.Annotations[BinderRefsAnnotation] = string(b)
}

// HasBinder reports whether the claim UID is among the bucket's binders.
func HasBinder(bucket *Bucket, uid types.UID) bool {
	for _, r := range GetBinderRefs(bucket) {
		if r.UID == string(uid) {
			return true
		}
	}
	return false
}

// GetBindingMode returns the recorded mode, defaulting to shared.
func GetBindingMode(bucket *Bucket) string {
	if m := bucket.Annotations[BindingModeAnnotation]; m != "" {
		return m
	}
	return BindingModeShared
}

// SetBindingMode records the mode chosen by the (first) binder.
func SetBindingMode(bucket *Bucket, exclusive bool) {
	if bucket.Annotations == nil {
		bucket.Annotations = map[string]string{}
	}
	mode := BindingModeShared
	if exclusive {
		mode = BindingModeExclusive
	}
	bucket.Annotations[BindingModeAnnotation] = mode
}

// RequireNewParameter, stamped into Bucket.Spec.Parameters when the creating
// claim set requireNew, tells the driver to refuse adopting a backend bucket
// that already exists (gRPC AlreadyExists), which the sidecar surfaces as a
// terminal refusal instead of a silent retry loop.
const RequireNewParameter = "cosi.lazedo.dev/require-new"
