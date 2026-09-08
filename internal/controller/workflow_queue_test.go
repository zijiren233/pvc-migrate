package controller

import (
	"context"
	"encoding/json"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestExecutionQueueExcludesInterruptedCutover(t *testing.T) {
	object := &v1alpha1.PodMigration{}

	object.Status.Phase = v1alpha1.WorkflowPhase(domain.PhaseFinalSyncing)
	if workflowQueuePredicate(
		false,
		func(crclient.Object) {},
	).Create(event.CreateEvent{Object: object}) {
		t.Fatal("interrupted cutover would wait behind new migrations after leader election")
	}
}

func TestRecoveryQueueRoutesPersistedCheckpointsAcrossKinds(t *testing.T) {
	for _, workflow := range domain.ControllerWorkflows() {
		for _, kind := range []domain.ControllerKind{workflow.Kind, workflow.ClusterKind} {
			if kind == "" {
				continue
			}

			t.Run(string(kind), func(t *testing.T) {
				for _, test := range []struct {
					phase    domain.Phase
					recovery bool
				}{
					{phase: domain.PhasePlanned},
					{phase: domain.PhaseWarmCopying},
					{phase: domain.PhaseCompleted},
					{phase: domain.PhasePausing, recovery: true},
					{phase: domain.PhaseFinalSyncing, recovery: true},
					{phase: domain.PhaseActivating, recovery: true},
					{phase: domain.PhaseResuming, recovery: true},
					{phase: domain.PhaseRollingBack, recovery: true},
				} {
					object := kube.WorkflowObjectForKind(kind)

					payload, err := json.Marshal(
						map[string]any{"status": map[string]any{"phase": test.phase}},
					)
					if err != nil {
						t.Fatal(err)
					}

					if err := json.Unmarshal(payload, object); err != nil {
						t.Fatal(err)
					}

					created := event.CreateEvent{Object: object}

					cancel := func(crclient.Object) {}
					if got := workflowRecoveryQueuePredicate(
						cancel,
					).Create(created); got != test.recovery {
						t.Fatalf("%s recovery routing=%v", test.phase, got)
					}

					if got := workflowQueuePredicate(
						false,
						cancel,
					).Create(created); got == test.recovery {
						t.Fatalf("%s execution routing=%v", test.phase, got)
					}
				}
			})
		}
	}
}

func TestRecoveryQueueAllowsExplicitResumeAndDeletionCancellation(t *testing.T) {
	before := &v1alpha1.PodMigration{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
	before.Status.Phase = v1alpha1.WorkflowPhase(domain.PhaseFailed)
	before.Status.ResumeFrom = v1alpha1.WorkflowPhase(domain.PhaseFinalSyncing)
	resumed := before.DeepCopy()
	resumed.Status.Phase = before.Status.ResumeFrom
	cancellations := 0

	p := workflowRecoveryQueuePredicate(func(crclient.Object) { cancellations++ })
	if !p.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: resumed}) {
		t.Fatal("explicit cutover resume did not wake recovery")
	}

	progress := resumed.DeepCopy()

	progress.Status.Phase = v1alpha1.WorkflowPhase(domain.PhaseResuming)
	if p.Update(event.UpdateEvent{ObjectOld: resumed, ObjectNew: progress}) {
		t.Fatal("ordinary recovery progress created a status feedback loop")
	}

	deleting := progress.DeepCopy()
	now := metav1.Now()

	deleting.DeletionTimestamp = &now
	if p.Update(event.UpdateEvent{ObjectOld: progress, ObjectNew: deleting}) || cancellations != 1 {
		t.Fatal("deletion must cancel recovery and move to the deletion queue")
	}

	if p.Create(event.CreateEvent{Object: deleting}) {
		t.Fatal("recovery queue accepted an already deleting workflow")
	}
}

type queueCopyService struct{ recordingWorkflowResumer }

func (s *queueCopyService) ResumeCopy(context.Context, *domain.Session) error {
	s.called = "copy"
	return nil
}

func TestDuplicateReconcilePreservesActiveCancellation(t *testing.T) {
	session := newRunnerSession("duplicate-copy")
	session.Generation = 1
	session.Status.ObservedGeneration = 1
	session.Status.Phase = domain.PhaseWarmCopying
	session.BackendUID = "active-uid"
	service := &queueCopyService{}
	r := NewWorkflowReconciler(service, &runnerSessionStore{latest: session})

	active, cancel := context.WithCancel(t.Context())
	defer cancel()

	r.activeWorkflows.Store(session.BackendUID, cancel)
	request := reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "system", Name: session.ID},
	}

	result, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy)
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("duplicate request must wait: result=%v error=%v", result, err)
	}

	if service.called != "" {
		t.Fatal("duplicate request entered the running workflow")
	}

	r.cancelWorkflow(&v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{UID: session.BackendUID}})

	if active.Err() != context.Canceled {
		t.Fatal("duplicate request removed the original worker's cancellation handle")
	}
}

func TestWorkflowQueuesRouteDeletionWithoutStatusFeedback(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		before := &v1alpha1.Copy{
			ObjectMeta: metav1.ObjectMeta{Name: "copy", UID: "uid", Generation: 1},
		}
		after := before.DeepCopy()
		now := metav1.Now()
		after.DeletionTimestamp = &now
		after.Generation++
		cancellations := 0

		p := workflowQueuePredicate(deleting, func(crclient.Object) { cancellations++ })
		if got := p.Create(event.CreateEvent{Object: before}); got == deleting {
			t.Fatalf("queue deletion=%t accepted wrong initial object", deleting)
		}

		if got := p.Create(event.CreateEvent{Object: after}); got != deleting {
			t.Fatalf("queue deletion=%t missed deleting initial object", deleting)
		}

		if got := p.Update(
			event.UpdateEvent{ObjectOld: before, ObjectNew: after},
		); got != deleting {
			t.Fatalf("queue deletion=%t routed update incorrectly", deleting)
		}

		if cancellations != 1 {
			t.Fatalf("queue deletion=%t did not cancel active execution", deleting)
		}

		status := after.DeepCopy()

		status.Status.Phase = "Aborting"
		if p.Update(event.UpdateEvent{ObjectOld: after, ObjectNew: status}) {
			t.Fatal("status write queued another finalization")
		}

		if p.Delete(event.DeleteEvent{Object: after}) {
			t.Fatal("deleted object was queued")
		}
	}
}
