package bucketaccess

import (
	"context"
	"encoding/json"
	"testing"

	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	fakebucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/fake"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	fakespec "sigs.k8s.io/container-object-storage-interface/proto/fake"
)

func s3Info(endpoint, ca string) cosiapi.BucketInfo {
	return cosiapi.BucketInfo{Spec: cosiapi.BucketInfoSpec{
		BucketName: "obs-loki",
		S3: &cosiapi.SecretS3{
			Endpoint: endpoint, Region: "us-east-1",
			AccessKeyID: "AK", AccessSecretKey: "SK", CACert: ca,
		},
	}}
}

// An S3 BucketInfo spelled out by upstream v1alpha2's names: the protocol, the
// bucket, the endpoint as given, path addressing, the keys and the instance's
// CA; and this fork's two extras: the bare host, https or not.
func TestFlatKeysSpellOutS3(t *testing.T) {
	got := flatKeys(s3Info("https://s3-client.minio.svc:9000/x", "PEM"))
	want := map[string]string{
		FlatProtocol: "S3", FlatBucketID: "obs-loki", FlatEndpoint: "https://s3-client.minio.svc:9000/x",
		FlatRegion: "us-east-1", FlatAddressingStyle: "path", FlatCA: "PEM",
		FlatAccessKeyID: "AK", FlatAccessSecretKey: "SK",
		FlatHost: "s3-client.minio.svc:9000", FlatInsecure: "false",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	for _, k := range []string{"COSI_PROTOCOL", "COSI_S3_BUCKET_ID", "COSI_S3_ENDPOINT", "COSI_S3_REGION",
		"COSI_S3_ADDRESSING_STYLE", "COSI_S3_ACCESS_KEY_ID", "COSI_S3_ACCESS_SECRET_KEY", "COSI_CERTIFICATE_AUTHORITY"} {
		if _, ok := got[k]; !ok {
			t.Errorf("upstream v1alpha2 key %s missing", k)
		}
	}
	if http := flatKeys(s3Info("http://minio:9000", "")); http[FlatInsecure] != "true" || http[FlatHost] != "minio:9000" {
		t.Errorf("plain http: %v", http)
	}
	if _, ok := flatKeys(s3Info("http://minio:9000", ""))[FlatCA]; ok {
		t.Error("no CA in the BucketInfo, no CA key")
	}
	if flatKeys(cosiapi.BucketInfo{}) != nil {
		t.Error("no S3, no flat keys")
	}
}

// A field the BucketInfo no longer has takes its key with it; the data that is
// not a flat key is left alone; an up-to-date secret reports no change.
func TestWithFlatKeysRemovesStale(t *testing.T) {
	data := map[string][]byte{"BucketInfo": []byte("{}"), FlatCA: []byte("OLD"), "other": []byte("x")}
	out, changed := withFlatKeys(data, s3Info("https://h:9000", ""))
	if !changed {
		t.Fatal("expected a change")
	}
	if _, ok := out[FlatCA]; ok {
		t.Error("a CA the BucketInfo no longer has must go")
	}
	if string(out["other"]) != "x" || string(out["BucketInfo"]) != "{}" {
		t.Errorf("non-flat keys touched: %v", out)
	}
	if _, again := withFlatKeys(out, s3Info("https://h:9000", "")); again {
		t.Error("an up-to-date secret must report no change")
	}
	if string(data[FlatCA]) != "OLD" {
		t.Error("the input data must not be mutated")
	}
}

// A granted access is not granted again: its secret gets the flat keys from
// its own BucketInfo, and the driver is never asked.
func TestBackfillFlatKeysOnGrantedAccess(t *testing.T) {
	const ns, driver = "logs", "minio"
	info := s3Info("https://s3-client.minio.svc:9000", "PEM")
	raw, _ := json.Marshal(info)
	bac := &v1alpha1.BucketAccessClass{ObjectMeta: metav1.ObjectMeta{Name: "minio-s3"}, DriverName: driver}
	ba := &v1alpha1.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{Name: "obs-loki", Namespace: ns},
		Spec: v1alpha1.BucketAccessSpec{BucketAccessClassName: "minio-s3", BucketClaimName: "obs-loki",
			CredentialsSecretName: "obs-loki-bucketinfo", Protocol: v1alpha1.ProtocolS3},
		Status: v1alpha1.BucketAccessStatus{AccessGranted: true, AccountID: "acct"},
	}
	secret := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "obs-loki-bucketinfo", Namespace: ns},
		Data: map[string][]byte{"BucketInfo": raw}}

	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverGrantBucketAccess = func(context.Context, *cosi.DriverGrantBucketAccessRequest,
		...grpc.CallOption) (*cosi.DriverGrantBucketAccessResponse, error) {
		t.Fatal("a granted access must not be granted again")
		return nil, nil
	}
	bal := BucketAccessListener{
		driverName:        driver,
		provisionerClient: &mpc,
		bucketClient:      fakebucketclientset.NewSimpleClientset(bac, ba),
		kubeClient:        fakekubeclientset.NewSimpleClientset(secret),
	}
	if err := bal.Add(context.TODO(), ba); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, _ := bal.secrets(ns).Get(context.TODO(), "obs-loki-bucketinfo", metav1.GetOptions{})
	for k, v := range flatKeys(info) {
		if string(got.Data[k]) != v {
			t.Errorf("%s = %q, want %q", k, got.Data[k], v)
		}
	}
	if string(got.Data["BucketInfo"]) != string(raw) {
		t.Error("the BucketInfo must stay as it was")
	}
}

// An access of another driver is not this sidecar's to touch.
func TestBackfillSkipsOtherDriver(t *testing.T) {
	const ns = "logs"
	raw, _ := json.Marshal(s3Info("https://h:9000", ""))
	bac := &v1alpha1.BucketAccessClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}, DriverName: "other"}
	ba := &v1alpha1.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: ns},
		Spec:       v1alpha1.BucketAccessSpec{BucketAccessClassName: "other", CredentialsSecretName: "s"},
		Status:     v1alpha1.BucketAccessStatus{AccessGranted: true, AccountID: "acct"},
	}
	secret := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: ns}, Data: map[string][]byte{"BucketInfo": raw}}
	bal := BucketAccessListener{
		driverName:   "minio",
		bucketClient: fakebucketclientset.NewSimpleClientset(bac, ba),
		kubeClient:   fakekubeclientset.NewSimpleClientset(secret),
	}
	if err := bal.Add(context.TODO(), ba); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, _ := bal.secrets(ns).Get(context.TODO(), "s", metav1.GetOptions{})
	if len(got.Data) != 1 {
		t.Errorf("another driver's secret was touched: %v", got.Data)
	}
}

// A new S3 grant writes the flat keys with the BucketInfo.
func TestGrantWritesFlatKeys(t *testing.T) {
	const ns, driver = "metrics", "minio"
	b := &v1alpha1.Bucket{ObjectMeta: metav1.ObjectMeta{Name: "obs-mimir"},
		Spec:   v1alpha1.BucketSpec{DriverName: driver, Protocols: []v1alpha1.Protocol{v1alpha1.ProtocolS3}},
		Status: v1alpha1.BucketStatus{BucketID: "id", BucketReady: true}}
	bc := &v1alpha1.BucketClaim{ObjectMeta: metav1.ObjectMeta{Name: "obs-mimir", Namespace: ns},
		Status: v1alpha1.BucketClaimStatus{BucketReady: true, BucketName: "obs-mimir"}}
	bac := &v1alpha1.BucketAccessClass{ObjectMeta: metav1.ObjectMeta{Name: "minio-s3"}, DriverName: driver,
		AuthenticationType: v1alpha1.AuthenticationTypeKey}
	ba := &v1alpha1.BucketAccess{ObjectMeta: metav1.ObjectMeta{Name: "obs-mimir", Namespace: ns},
		Spec: v1alpha1.BucketAccessSpec{BucketClaimName: "obs-mimir", BucketAccessClassName: "minio-s3",
			CredentialsSecretName: "obs-mimir-bucketinfo", Protocol: v1alpha1.ProtocolS3}}

	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverGrantBucketAccess = func(context.Context, *cosi.DriverGrantBucketAccessRequest,
		...grpc.CallOption) (*cosi.DriverGrantBucketAccessResponse, error) {
		return &cosi.DriverGrantBucketAccessResponse{AccountId: "acct", Credentials: map[string]*cosi.CredentialDetails{
			"s3": {Secrets: map[string]string{"endpoint": "https://s3-client.minio.svc:9000",
				"accessKeyID": "AK", "accessSecretKey": "SK", "ca.crt": "PEM"}},
		}}, nil
	}
	bal := BucketAccessListener{
		driverName:        driver,
		provisionerClient: &mpc,
		bucketClient:      fakebucketclientset.NewSimpleClientset(b, bc, bac, ba),
		kubeClient:        fakekubeclientset.NewSimpleClientset(),
	}
	if err := bal.Add(context.TODO(), ba); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := bal.secrets(ns).Get(context.TODO(), "obs-mimir-bucketinfo", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("secret: %v", err)
	}
	for k, v := range map[string]string{FlatBucketID: "obs-mimir", FlatHost: "s3-client.minio.svc:9000",
		FlatInsecure: "false", FlatAccessKeyID: "AK", FlatAccessSecretKey: "SK", FlatCA: "PEM"} {
		if string(got.Data[k]) != v {
			t.Errorf("%s = %q, want %q", k, got.Data[k], v)
		}
	}
	if len(got.Data["BucketInfo"]) == 0 {
		t.Error("the BucketInfo must be there too")
	}
}
