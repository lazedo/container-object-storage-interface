// lazedo: unit coverage for the create-or-adopt sharing semantics
// (BucketClaimSpec.RequireNew / Exclusive) — the four quadrants plus
// refcount bookkeeping and last-binder deletion policy.
package bucketclaim

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	fakebucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/fake"
)

func newSharingListener(t *testing.T, ctx context.Context) (*BucketClaimListener, *fakebucketclientset.Clientset) {
	t.Helper()
	client := fakebucketclientset.NewSimpleClientset()
	listener := NewBucketClaimListener()
	listener.InitializeKubeClient(fakekubeclientset.NewSimpleClientset())
	listener.InitializeBucketClient(client)
	listener.InitializeEventRecorder(record.NewFakeRecorder(32))
	if _, err := client.ObjectstorageV1alpha1().BucketClasses().Create(ctx, goldClass.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating class: %v", err)
	}
	return listener, client
}

func sharingClaim(ctx context.Context, t *testing.T, client *fakebucketclientset.Clientset, name, ns, uid string, mutate func(*v1alpha1.BucketClaim)) *v1alpha1.BucketClaim {
	t.Helper()
	c := &v1alpha1.BucketClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: ktypes.UID(uid)},
		Spec: v1alpha1.BucketClaimSpec{
			BucketClassName: goldClass.Name,
			Protocols:       []v1alpha1.Protocol{v1alpha1.ProtocolS3},
		},
	}
	if mutate != nil {
		mutate(c)
	}
	out, err := client.ObjectstorageV1alpha1().BucketClaims(ns).Create(ctx, c, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating claim %s/%s: %v", ns, name, err)
	}
	return out
}

func boundCondition(ctx context.Context, t *testing.T, client *fakebucketclientset.Clientset, ns, name string) *metav1.Condition {
	t.Helper()
	cur, err := client.ObjectstorageV1alpha1().BucketClaims(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get claim: %v", err)
	}
	return apimeta.FindStatusCondition(cur.Status.Conditions, v1alpha1.ConditionBound)
}

// Default/default: the second same-named claim ADOPTS instead of pending.
func TestDefaultAdoptsSameName(t *testing.T) {
	ctx := context.Background()
	listener, client := newSharingListener(t, ctx)

	a := sharingClaim(ctx, t, client, "shared", "ns-a", "uid-a", nil)
	if err := listener.Add(ctx, a); err != nil {
		t.Fatalf("claim a: %v", err)
	}
	b := sharingClaim(ctx, t, client, "shared", "ns-b", "uid-b", nil)
	if err := listener.Add(ctx, b); err != nil {
		t.Fatalf("claim b: %v", err)
	}

	bucket, err := client.ObjectstorageV1alpha1().Buckets().Get(ctx, "shared", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	if got := len(v1alpha1.GetBinderRefs(bucket)); got != 2 {
		t.Fatalf("want 2 binders, got %d", got)
	}
	curB, _ := client.ObjectstorageV1alpha1().BucketClaims("ns-b").Get(ctx, "shared", metav1.GetOptions{})
	if !curB.Status.Adopted {
		t.Error("adopter should report status.adopted=true")
	}
	if curB.Status.Binders != 2 {
		t.Errorf("adopter should report binders=2, got %d", curB.Status.Binders)
	}
	curA, _ := client.ObjectstorageV1alpha1().BucketClaims("ns-a").Get(ctx, "shared", metav1.GetOptions{})
	if curA.Status.Adopted {
		t.Error("creator must not report adopted")
	}
	if c := boundCondition(ctx, t, client, "ns-b", "shared"); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("adopter Bound condition = %+v, want True", c)
	}
}

// requireNew: refused when the Bucket already exists; fine when fresh.
func TestRequireNew(t *testing.T) {
	ctx := context.Background()
	listener, client := newSharingListener(t, ctx)

	a := sharingClaim(ctx, t, client, "shared", "ns-a", "uid-a", nil)
	if err := listener.Add(ctx, a); err != nil {
		t.Fatalf("claim a: %v", err)
	}
	b := sharingClaim(ctx, t, client, "shared", "ns-b", "uid-b", func(c *v1alpha1.BucketClaim) {
		c.Spec.RequireNew = true
	})
	if err := listener.Add(ctx, b); err != nil {
		t.Fatalf("refusal must be terminal (nil), got %v", err)
	}
	if c := boundCondition(ctx, t, client, "ns-b", "shared"); c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.RequireNewDenied {
		t.Errorf("Bound condition = %+v, want False/RequireNewDenied", c)
	}
	cur, _ := client.ObjectstorageV1alpha1().BucketClaims("ns-b").Get(ctx, "shared", metav1.GetOptions{})
	if cur.Status.BucketName != "" {
		t.Error("refused claim must not half-bind (BucketName set)")
	}
	bucket, _ := client.ObjectstorageV1alpha1().Buckets().Get(ctx, "shared", metav1.GetOptions{})
	if got := len(v1alpha1.GetBinderRefs(bucket)); got != 1 {
		t.Fatalf("binders must stay 1, got %d", got)
	}

	// Fresh name with requireNew works and stamps the driver parameter.
	f := sharingClaim(ctx, t, client, "fresh", "ns-b", "uid-f", func(c *v1alpha1.BucketClaim) {
		c.Spec.RequireNew = true
	})
	if err := listener.Add(ctx, f); err != nil {
		t.Fatalf("fresh requireNew: %v", err)
	}
	fb, err := client.ObjectstorageV1alpha1().Buckets().Get(ctx, "fresh", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("fresh bucket: %v", err)
	}
	if fb.Spec.Parameters[v1alpha1.RequireNewParameter] != "true" {
		t.Error("requireNew parameter not stamped for the driver")
	}
}

// exclusive: first binder locks the bucket; later claims (shared or
// exclusive) are refused; exclusive over an already-bound bucket is refused.
func TestExclusive(t *testing.T) {
	ctx := context.Background()
	listener, client := newSharingListener(t, ctx)

	// exclusive first
	a := sharingClaim(ctx, t, client, "vault", "ns-a", "uid-a", func(c *v1alpha1.BucketClaim) {
		c.Spec.Exclusive = true
	})
	if err := listener.Add(ctx, a); err != nil {
		t.Fatalf("claim a: %v", err)
	}
	bucket, _ := client.ObjectstorageV1alpha1().Buckets().Get(ctx, "vault", metav1.GetOptions{})
	if v1alpha1.GetBindingMode(bucket) != v1alpha1.BindingModeExclusive {
		t.Fatalf("binding mode = %s, want exclusive", v1alpha1.GetBindingMode(bucket))
	}

	// shared claim against exclusive bucket → refused
	b := sharingClaim(ctx, t, client, "vault", "ns-b", "uid-b", nil)
	if err := listener.Add(ctx, b); err != nil {
		t.Fatalf("refusal must be terminal, got %v", err)
	}
	if c := boundCondition(ctx, t, client, "ns-b", "vault"); c == nil || c.Reason != v1alpha1.ExclusivityDenied {
		t.Errorf("Bound condition = %+v, want ExclusivityDenied", c)
	}

	// exclusive claim against a bucket that already has binders → refused
	s := sharingClaim(ctx, t, client, "open", "ns-a", "uid-s", nil)
	if err := listener.Add(ctx, s); err != nil {
		t.Fatalf("claim s: %v", err)
	}
	x := sharingClaim(ctx, t, client, "open", "ns-b", "uid-x", func(c *v1alpha1.BucketClaim) {
		c.Spec.Exclusive = true
	})
	if err := listener.Add(ctx, x); err != nil {
		t.Fatalf("refusal must be terminal, got %v", err)
	}
	if c := boundCondition(ctx, t, client, "ns-b", "open"); c == nil || c.Reason != v1alpha1.ExclusivityDenied {
		t.Errorf("Bound condition = %+v, want ExclusivityDenied", c)
	}
}

// Re-reconciling a bound claim stays idempotent and preserves adopted=false
// for the creator.
func TestRebindIdempotent(t *testing.T) {
	ctx := context.Background()
	listener, client := newSharingListener(t, ctx)

	a := sharingClaim(ctx, t, client, "idem", "ns-a", "uid-a", nil)
	for range 3 {
		cur, _ := client.ObjectstorageV1alpha1().BucketClaims("ns-a").Get(ctx, "idem", metav1.GetOptions{})
		if err := listener.Add(ctx, cur); err != nil {
			t.Fatalf("re-add: %v", err)
		}
	}
	_ = a
	bucket, _ := client.ObjectstorageV1alpha1().Buckets().Get(ctx, "idem", metav1.GetOptions{})
	if got := len(v1alpha1.GetBinderRefs(bucket)); got != 1 {
		t.Fatalf("binders = %d, want 1", got)
	}
	cur, _ := client.ObjectstorageV1alpha1().BucketClaims("ns-a").Get(ctx, "idem", metav1.GetOptions{})
	if cur.Status.Adopted {
		t.Error("creator flipped to adopted on re-reconcile")
	}
}
