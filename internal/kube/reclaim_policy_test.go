package kube

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestDecodeWorkflowUsesCurrentPoliciesAndFrozenStorageIdentities(t *testing.T) {
	session := storeTestSession()
	session.Spec.SourcePVReclaimPolicy = "Retain"
	session.Spec.DestinationPVCReclaimPolicy = "Retain"

	object, ok := sessionObjectFor(session).(*v1alpha1.Migration)
	if !ok {
		t.Fatal("expected Migration")
	}

	if !setWorkflowStatus(object, session.Spec, session.Status, true) {
		t.Fatal("failed to checkpoint plan")
	}

	object.Spec.SourcePVReclaimPolicy = "Delete"
	object.Spec.DestinationPVCReclaimPolicy = "Delete"
	object.Generation++

	decoded, err := DecodeWorkflow(object)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Spec.SourcePVReclaimPolicy != "Delete" ||
		decoded.Spec.DestinationPVCReclaimPolicy != "Delete" {
		t.Fatalf("stale policy: %+v", decoded.Spec.SessionCommon)
	}

	if decoded.Spec.Volumes[0].SourcePV.UID != session.Spec.Volumes[0].SourcePV.UID ||
		decoded.Spec.Volumes[0].SourcePV.Name != session.Spec.Volumes[0].SourcePV.Name || decoded.PlanPending {
		t.Fatalf("policy edit changed storage checkpoint: %+v", decoded)
	}

	if object.Status.Plan.SourcePVReclaimPolicy != "Retain" {
		t.Fatal("decoding rewrote the frozen plan")
	}

	object.Spec.SourcePVReclaimPolicy = ""
	object.Spec.DestinationPVCReclaimPolicy = ""

	decoded, err = DecodeWorkflow(object)
	if err != nil || decoded.Spec.SourcePVReclaimPolicy != "" ||
		decoded.Spec.DestinationPVCReclaimPolicy != "" {
		t.Fatalf("removing policy must restore default Retain: %+v %v", decoded, err)
	}

	if err := domain.ValidateReclaimPolicies(
		decoded.Spec.SourcePVReclaimPolicy,
		decoded.Spec.DestinationPVCReclaimPolicy,
	); err != nil {
		t.Fatal(err)
	}
}
