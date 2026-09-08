package app

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func pendingCopy(t *testing.T) *domain.Session {
	t.Helper()

	session, err := kube.DecodeWorkflow(&v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pending",
			Namespace:       "app",
			UID:             "workflow",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.CopySpec{
			Volumes: []v1alpha1.VolumeRequest{
				{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return session
}

func TestUnplannedLifecycleNeverTouchesStorage(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseFailed, domain.PhaseAborted} {
		t.Run(string(phase), func(t *testing.T) {
			session := pendingCopy(t)
			session.Status.Phase = phase
			session.Status.ResumeFrom = domain.PhasePlanned
			store := &deletionStore{latest: session}
			client := fake.NewClientset()

			service := NewService(client, store, nil, nil, nil, nil, Config{})
			if phase != domain.PhaseAborted {
				if err := service.ValidateCopyResume(t.Context(), session); err != nil {
					t.Fatal(err)
				}
			}

			for range 2 {
				if err := service.ValidateCopyAbort(t.Context(), session); err != nil {
					t.Fatal(err)
				}

				if err := service.AbortCopy(t.Context(), session); err != nil {
					t.Fatal(err)
				}
			}

			if session.Status.Phase != domain.PhaseAborted {
				t.Fatal("abort did not stop request")
			}

			if err := service.ValidateCopyResume(t.Context(), session); err == nil {
				t.Fatal("aborted request can resume")
			}

			options := CleanupOptions{DeleteSession: true}
			if err := service.ValidateCopyCleanup(t.Context(), session, options); err != nil {
				t.Fatal(err)
			}

			if err := service.CleanupCopy(t.Context(), session, options); err != nil {
				t.Fatal(err)
			}

			if store.deletes != 1 || len(client.Actions()) != 0 {
				t.Fatalf("deletes=%d actions=%v", store.deletes, client.Actions())
			}
		})
	}
}

func TestUnplannedLifecycleRejectsPlanningRace(t *testing.T) {
	for _, planned := range []bool{false, true} {
		session := pendingCopy(t)
		latest := *session
		latest.ResourceVersion = "2"
		latest.PlanPending = !planned
		store := &deletionStore{latest: &latest}

		service := NewService(fake.NewClientset(), store, nil, nil, nil, nil, Config{})
		for _, err := range []error{
			service.AbortCopy(t.Context(), session),
			service.CleanupCopy(t.Context(), session, CleanupOptions{DeleteSession: true}),
		} {
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("expected conflict, got %v", err)
			}
		}

		if store.updates != 0 || store.deletes != 0 {
			t.Fatal("stale request mutated workflow")
		}
	}
}
