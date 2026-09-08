package kube

import (
	"context"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Execution-stage fixtures explicitly checkpoint a controller-discovered plan.
func createPlannedTestWorkflow(
	t *testing.T,
	ctx context.Context,
	store *CRDSessionStore,
	session *domain.Session,
) {
	t.Helper()

	spec, status := session.Spec, session.Status
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	session.Spec, session.Status, session.PlanPending = spec, status, false
	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}
}

func TestCRDCreatePreservesMinimalIntent(t *testing.T) {
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)

	session, err := DecodeWorkflow(&v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "minimal", Namespace: "app"},
		Spec: v1alpha1.CopySpec{
			Volumes: []v1alpha1.VolumeRequest{
				{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Create(t.Context(), session); err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Copy{}
	if err := client.Get(
		t.Context(),
		crclient.ObjectKey{Name: "minimal", Namespace: "app"},
		object,
	); err != nil {
		t.Fatal(err)
	}

	volume := object.Spec.Volumes[0]
	if volume.SourcePV != nil || volume.DestinationPVC != nil {
		t.Fatalf("submission manufactured identity constraints: %+v", volume)
	}
}
