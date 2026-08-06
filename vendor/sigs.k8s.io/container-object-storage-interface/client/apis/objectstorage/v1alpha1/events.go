package v1alpha1

// COSI relevant event reasons
const (
	FailedCreateBucket = "FailedCreateBucket"
	FailedDeleteBucket = "FailedDeleteBucket"
	WaitingForBucket   = "WaitingForBucket"

	FailedGrantAccess  = "FailedGrantAccess"
	FailedRevokeAccess = "FailedRevokeAccess"

	// lazedo: sharing-semantics events (see BucketClaimSpec.RequireNew /
	// Exclusive). AdoptedExistingBucket is informational anti-footgun
	// visibility; the others are terminal refusals.
	AdoptedExistingBucket = "AdoptedExistingBucket"
	RequireNewDenied      = "RequireNewDenied"
	ExclusivityDenied     = "ExclusivityDenied"
	ClassMismatch         = "ClassMismatch"
)

// lazedo: BucketClaim condition vocabulary. A single condition type keeps the
// contract simple: Bound=True when the claim is bound to a bucket; Bound=False
// with one of the refusal reasons above when provisioning was refused
// terminally (the claim never goes silently pending on a refusal).
const (
	// ConditionBound reports the claim's binding outcome.
	ConditionBound = "Bound"

	// ReasonBound is the happy Bound=True reason.
	ReasonBound = "Bound"
)
