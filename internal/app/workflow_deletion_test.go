package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
)

type deletionStore struct {
	memoryStore
	latest *domain.Session
	getErr error
}

func (*deletionStore) StorageBackend() string { return kube.SessionBackendCRD }

func (s *deletionStore) GetByKind(
	context.Context,
	string,
	string,
	domain.ControllerKind,
) (*domain.Session, error) {
	return s.latest, s.getErr
}

type vanishedDeletionStore struct {
	deletionStore
	lock *vanishedDeletionLock
}

func (s *vanishedDeletionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return s.lock, nil
}

type vanishedDeletionLock struct {
	fakeSessionLock
	deletes   int
	deleteErr error
}

func (l *vanishedDeletionLock) Delete(context.Context) error {
	l.deletes++
	return l.deleteErr
}

func TestDeletionRemovesLockWhenWorkflowDisappears(t *testing.T) {
	missing := apierrors.NewNotFound(
		schema.GroupResource{Group: "migrate.sealos.io", Resource: "copies"},
		"gone",
	)
	unavailable := errors.New("API unavailable")

	deleteFailure := errors.New("Lease deletion failed")
	for _, test := range []struct {
		name      string
		readErr   error
		deleteErr error
		wantErr   error
		deletes   int
	}{
		{"already deleted", missing, nil, nil, 1},
		{"delete failure", missing, deleteFailure, deleteFailure, 1},
		{"read failure", unavailable, nil, unavailable, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			lock := &vanishedDeletionLock{deleteErr: test.deleteErr}
			store := &vanishedDeletionStore{
				deletionStore: deletionStore{getErr: test.readErr},
				lock:          lock,
			}
			client := fake.NewClientset()
			service := NewService(client, store, nil, nil, nil, nil, Config{})
			session := appTestSession()
			session.Deleting = true

			err := service.FinalizeDeletedWorkflow(t.Context(), session)
			if !errors.Is(err, test.wantErr) || lock.deletes != test.deletes ||
				len(client.Actions()) != 0 ||
				store.updates != 0 ||
				store.deletes != 0 {
				t.Fatalf(
					"err=%v lock deletes=%d store=%+v actions=%v",
					err,
					lock.deletes,
					store.deletionStore,
					client.Actions(),
				)
			}
		})
	}
}

func (*deletionStore) CheckWorkflowNameCollision(
	context.Context,
	*domain.Session,
) error {
	return nil
}

func (*deletionStore) EnsureSessionProtection(context.Context, *domain.Session) error { return nil }

func TestDeleteBeforeCopyStartsPreservesSource(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseFailed, domain.PhaseAborted} {
		t.Run(string(phase), func(t *testing.T) {
			session := appTestSession()
			setSessionOperation(session, domain.OperationCopy)
			session.Status.Phase = phase
			session.Status.ResumeFrom = domain.PhasePlanned
			session.Deleting = true
			session.BackendUID = "workflow-uid"
			volume := session.Spec.Volumes[0]
			client := fake.NewClientset(
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Namespace: volume.SourcePVC.Namespace,
					Name:      volume.SourcePVC.Name,
					UID:       volume.SourcePVC.UID,
				}},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: volume.SourcePV.Name,
						UID:  volume.SourcePV.UID,
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						ClaimRef: &corev1.ObjectReference{
							Namespace: volume.SourcePVC.Namespace,
							Name:      volume.SourcePVC.Name,
							UID:       volume.SourcePVC.UID,
						},
					},
				},
			)
			store := &deletionStore{latest: session}

			service := NewService(client, store, nil, nil, nil, nil, Config{})
			if err := service.FinalizeDeletedWorkflow(t.Context(), session); err != nil {
				t.Fatal(err)
			}

			if session.Status.Phase != domain.PhaseAborted || store.deletes != 1 {
				t.Fatalf("phase=%s deletes=%d", session.Status.Phase, store.deletes)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" || action.GetVerb() == "update" ||
					action.GetVerb() == "patch" {
					t.Fatalf("source mutated: %v", action)
				}
			}
		})
	}
}

func TestDeletionRejectsReplacedWorkflowOrWithdrawnIntent(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		session := appTestSession()
		session.BackendUID = "original"
		latest := appTestSession()
		latest.BackendUID = "replacement"

		latest.Deleting = replaced
		if !replaced {
			latest.BackendUID = session.BackendUID
		}

		store := &deletionStore{latest: latest}

		service := NewService(fake.NewClientset(), store, nil, nil, nil, nil, Config{})
		if err := service.FinalizeDeletedWorkflow(
			t.Context(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("error=%v", err)
		}

		if store.updates != 0 || store.deletes != 0 {
			t.Fatal("changed workflow was mutated")
		}
	}
}

func TestAbortRejectsInProgressActivation(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseActivating, domain.PhaseResuming} {
		session := appTestSession()
		session.Status.Phase = phase
		session.Status.ResumeFrom = ""

		service := &Service{now: time.Now}
		if err := service.abort(
			t.Context(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("phase=%s error=%v", phase, err)
		}

		if session.Status.Phase != phase {
			t.Fatal("abort changed activation checkpoint")
		}
	}
}

func TestAbortRejectsInterruptedPVCIdentity(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationRename, domain.OperationMove} {
		for _, failed := range []bool{false, true} {
			session := appTestSession()
			setSessionOperation(session, operation)

			phase := domain.PhaseRenaming
			if operation == domain.OperationMove {
				phase = domain.PhaseMoving
			}

			session.Status.Phase = phase
			if failed {
				session.Status.Phase = domain.PhaseFailed
				session.Status.ResumeFrom = phase
			}

			service := &Service{now: time.Now}
			if err := service.validateAbort(
				t.Context(),
				session,
			); domain.CategoryOf(
				err,
			) != domain.ErrorPrecondition {
				t.Fatalf("operation=%s failed=%v error=%v", operation, failed, err)
			}

			if !deletionRequiresConvergence(phase) {
				t.Fatalf("deletion would abort %s", phase)
			}
		}
	}
}

func TestDeletionSpecConflictStopsBeforeRecovery(t *testing.T) {
	for _, test := range []struct {
		operation domain.Operation
		phases    []domain.Phase
	}{
		{domain.OperationMigrate, []domain.Phase{domain.PhaseActivating, domain.PhaseRollingBack}},
		{domain.OperationRename, []domain.Phase{domain.PhaseRenaming, domain.PhaseRollingBack}},
		{domain.OperationMove, []domain.Phase{domain.PhaseMoving, domain.PhaseRollingBack}},
	} {
		for _, phase := range test.phases {
			for _, failed := range []bool{false, true} {
				session := appTestSession()
				setSessionOperation(session, test.operation)
				session.Deleting = true
				session.BackendUID = "workflow-uid"
				session.Generation = 2
				session.Status.ObservedGeneration = 1

				session.Status.Phase = phase
				if failed {
					session.Status.Phase = domain.PhaseFailed
					session.Status.ResumeFrom = phase
				}

				latest := *session
				latest.Generation++
				store := &deletionStore{latest: &latest}
				client := fake.NewClientset()
				service := NewService(client, store, nil, nil, nil, nil, Config{})

				err := service.FinalizeDeletedWorkflow(t.Context(), session)
				if domain.CategoryOf(err) != domain.ErrorConflict || store.updates != 0 ||
					store.deletes != 0 || len(client.Actions()) != 0 || session.Generation != 2 {
					t.Fatalf(
						"operation=%s phase=%s failed=%v: recovery ran after conflict: %v",
						test.operation,
						phase,
						failed,
						err,
					)
				}
			}
		}
	}
}
