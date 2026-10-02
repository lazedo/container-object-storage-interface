/*
lazedo: flat keys -- the BucketInfo of an S3 access, spelled out one field per
key next to it in the same secret, by the names upstream COSI v1alpha2 gives
them (client/apis/objectstorage/v1alpha2/protocols.go: COSI_PROTOCOL,
COSI_S3_BUCKET_ID, COSI_S3_ENDPOINT, ...).

BucketInfo is one JSON document, which suits a consumer that parses it (the
csi-s3 driver, the kazoo-operator) and nothing else: a workload that takes its
S3 settings from the environment (`envFrom: secretRef`) -- loki and mimir with
-config.expand-env -- would need the keys copied out of it, and a copy is a
snapshot that breaks on the next rotation. v1alpha2 drops BucketInfo for these
keys; here they come NEXT to it, so its consumers keep working, and a consumer
written for them keeps working when this fork moves to v1alpha2. They are
DERIVED from the BucketInfo, never minted: written with it on a grant,
rewritten with it when it changes, and backfilled from it on secrets written
before they existed.

Two keys are this fork's, outside the COSI_ prefix upstream reserves: S3_HOST
(the endpoint without scheme or path, the form mimir takes -- it refuses a URL)
and S3_INSECURE (whether that endpoint is plain http).
COSI_S3_ADDRESSING_STYLE is `path`: the v1alpha1 driver does not say, and path
is how MinIO is addressed in the cluster.
*/

package bucketaccess

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

// The flat keys: upstream v1alpha2's names, and this fork's two extras.
const (
	FlatProtocol        = "COSI_PROTOCOL"
	FlatCA              = "COSI_CERTIFICATE_AUTHORITY"
	FlatBucketID        = "COSI_S3_BUCKET_ID"
	FlatEndpoint        = "COSI_S3_ENDPOINT"
	FlatRegion          = "COSI_S3_REGION"
	FlatAddressingStyle = "COSI_S3_ADDRESSING_STYLE"
	FlatAccessKeyID     = "COSI_S3_ACCESS_KEY_ID"
	FlatAccessSecretKey = "COSI_S3_ACCESS_SECRET_KEY"
	FlatHost            = "S3_HOST"
	FlatInsecure        = "S3_INSECURE"
)

// allFlatKeys is every key this file owns in a secret: the ones a BucketInfo
// no longer has are removed, never left stale.
var allFlatKeys = []string{
	FlatProtocol, FlatCA, FlatBucketID, FlatEndpoint, FlatRegion, FlatAddressingStyle,
	FlatAccessKeyID, FlatAccessSecretKey, FlatHost, FlatInsecure,
}

// flatKeys spells out an S3 BucketInfo; nil for anything else.
func flatKeys(info cosiapi.BucketInfo) map[string]string {
	s3 := info.Spec.S3
	if s3 == nil {
		return nil
	}
	out := map[string]string{
		FlatProtocol:        "S3",
		FlatBucketID:        info.Spec.BucketName,
		FlatEndpoint:        s3.Endpoint,
		FlatAddressingStyle: "path",
		FlatAccessKeyID:     s3.AccessKeyID,
		FlatAccessSecretKey: s3.AccessSecretKey,
		FlatHost:            hostOf(s3.Endpoint),
		FlatInsecure:        strconv.FormatBool(strings.HasPrefix(s3.Endpoint, "http://")),
	}
	if s3.Region != "" {
		out[FlatRegion] = s3.Region
	}
	if s3.CACert != "" {
		out[FlatCA] = s3.CACert
	}
	return out
}

// hostOf is an endpoint without its scheme or path -- host[:port], the form
// mimir takes (it refuses a URL; https or not is S3_INSECURE).
func hostOf(endpoint string) string {
	h := endpoint
	if i := strings.Index(h, "://"); i >= 0 {
		h = h[i+3:]
	}
	if i := strings.IndexByte(h, '/'); i >= 0 {
		h = h[:i]
	}
	return h
}

// withFlatKeys returns a copy of the secret data with the flat keys of info in
// place -- set, refreshed, and the ones info no longer has removed -- and
// whether anything differs from data.
func withFlatKeys(data map[string][]byte, info cosiapi.BucketInfo) (map[string][]byte, bool) {
	want := flatKeys(info)
	out := make(map[string][]byte, len(data)+len(want))
	for k, v := range data {
		out[k] = v
	}
	changed := false
	for _, k := range allFlatKeys {
		v, ok := want[k]
		cur, had := out[k]
		switch {
		case ok && (!had || string(cur) != v):
			out[k] = []byte(v)
			changed = true
		case !ok && had:
			delete(out, k)
			changed = true
		}
	}
	return out, changed
}

// flatKeysStale reports whether the flat keys in the secret data differ from
// what info spells out.
func flatKeysStale(data map[string][]byte, info cosiapi.BucketInfo) bool {
	_, changed := withFlatKeys(data, info)
	return changed
}

// backfillFlatKeys brings the flat keys of a granted access's secret in line
// with its own BucketInfo. A granted access is not granted again (that would
// mint new credentials), so secrets written before the flat keys existed get
// them here: derived from what the secret already holds, nothing minted.
func (bal *BucketAccessListener) backfillFlatKeys(ctx context.Context, ba *v1alpha1.BucketAccess) error {
	name := ba.Spec.CredentialsSecretName
	if name == "" {
		return nil
	}
	class, err := bal.bucketAccessClasses().Get(ctx, ba.Spec.BucketAccessClassName, metav1.GetOptions{})
	if kubeerrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !strings.EqualFold(class.DriverName, bal.driverName) {
		return nil
	}
	ns := ba.ObjectMeta.Namespace
	secret, err := bal.secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if kubeerrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	raw, ok := secret.Data["BucketInfo"]
	if !ok {
		return nil
	}
	var info cosiapi.BucketInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		klog.V(3).ErrorS(err, "BucketInfo unreadable, flat keys not backfilled",
			"bucketAccess", ba.ObjectMeta.Name, "secret", name)
		return nil
	}
	data, changed := withFlatKeys(secret.Data, info)
	if !changed {
		return nil
	}
	cur := secret.DeepCopy()
	cur.Data = data
	cur.StringData = nil
	if _, err := bal.secrets(ns).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
		return err
	}
	klog.V(3).InfoS("Flat keys backfilled from BucketInfo",
		"bucketAccess", ba.ObjectMeta.Name, "secret", name)
	return nil
}
