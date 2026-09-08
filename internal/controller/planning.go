package controller

import (
	"context"
	"errors"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type WorkflowPlanner func(context.Context, *domain.Session, string) (domain.SessionSpec, error)

func (r *Runner) WithPlanner(planner WorkflowPlanner) *Runner {
	r.planner = planner
	return r
}

func (r *WorkflowReconciler) WithPlanner(planner WorkflowPlanner) *WorkflowReconciler {
	r.planner = planner
	return r
}

func (r *WorkflowReconciler) planWorkflow(
	ctx context.Context,
	session *domain.Session,
) (resultErr error) {
	if r.planner == nil {
		return errors.New("controller workflow planner is not configured")
	}

	if err := kube.RequireNamespace(ctx, r.kubeClient, session.Spec.SessionNamespace); err != nil {
		return r.failPlanning(ctx, session, err)
	}

	lock, err := kube.AcquireRequiredSessionLock(
		ctx,
		r.store,
		session.Spec.SessionNamespace,
		kube.SessionLockID(session),
	)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		resultErr = errors.Join(resultErr, lock.Release(releaseCtx))
	}()

	ctx, cancel := lock.Bind(ctx)
	defer cancel()

	latest, err := r.store.GetByKind(
		ctx,
		session.Spec.SessionNamespace,
		session.ID,
		session.BackendResource,
	)
	if kube.IsSessionNotFound(err) {
		return lock.Delete(ctx)
	}

	if err != nil {
		return err
	}

	if latest.Deleting {
		return lock.Delete(ctx)
	}

	if latest.BackendUID != session.BackendUID || !latest.PlanPending ||
		latest.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if latest.Spec.SessionNamespace != session.Spec.SessionNamespace {
		return domain.NewError(
			domain.ErrorConflict,
			"plan workflow",
			"session namespace changed while acquiring planning lock",
		)
	}

	*session = *latest

	spec, err := r.planner(ctx, session, r.trustedToolImage)
	if err != nil {
		return r.failPlanning(ctx, session, err)
	}

	if err := lock.Err(); err != nil {
		return err
	}

	planned := domain.NewSession(session.ID, spec, time.Now())
	session.Spec = planned.Spec
	session.Status = planned.Status

	session.PlanPending = false
	if workload := session.Spec.Workload(); workload.Adapter == domain.WorkloadStandalone &&
		len(workload.OriginalObject) > 0 {
		session.Status.OriginalPodSnapshotHash = podSnapshotHash(workload.OriginalObject)
	}

	session.SetCondition(
		domain.Condition{
			Type:               "Planned",
			Status:             metav1.ConditionTrue,
			Reason:             "DiscoverySucceeded",
			Message:            "Resource identities and execution plan resolved",
			LastTransitionTime: metav1.Now(),
		},
	)

	if err := r.store.Update(ctx, session); err != nil {
		// A failed commit must never make an unpersisted snapshot executable.
		*session = *latest
		return err
	}

	r.recordPlanning(
		session,
		"Normal",
		"DiscoverySucceeded",
		"Resource identities and execution plan resolved",
	)

	return nil
}

func (r *WorkflowReconciler) failPlanning(
	ctx context.Context,
	session *domain.Session,
	cause error,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhasePlanned
	session.Status.FailureReason = ""
	session.Status.Message = domain.BoundWorkflowMessage(cause.Error())
	session.Status.ErrorCategory = domain.CategoryOf(cause)
	session.Status.UpdatedAt = metav1.Now()
	session.SetCondition(
		domain.Condition{
			Type:               "Planned",
			Status:             metav1.ConditionFalse,
			Reason:             "DiscoveryFailed",
			Message:            session.Status.Message,
			LastTransitionTime: metav1.Now(),
		},
	)

	if err := r.store.Update(ctx, session); err != nil {
		return err
	}

	r.recordPlanning(session, "Warning", "DiscoveryFailed", session.Status.Message)

	return nil
}

func (r *WorkflowReconciler) recordPlanning(
	session *domain.Session,
	eventType, reason, message string,
) {
	r.logger.Info(
		"workflow planning",
		"workflow",
		session.ID,
		"kind",
		session.BackendResource,
		"reason",
		reason,
		"message",
		message,
	)

	if r.recorder == nil {
		return
	}

	object := kube.WorkflowObjectForKind(session.BackendResource)
	object.SetName(session.ID)

	if !domain.IsClusterControllerKind(session.BackendResource) {
		object.SetNamespace(session.Spec.SourceNamespace)
	}

	object.SetUID(session.BackendUID)
	r.recorder.Eventf(object, nil, eventType, reason, "Plan", "%s", message)
}

func (r *WorkflowReconciler) deleteUnplannedWorkflow(
	ctx context.Context,
	session *domain.Session,
) (resultErr error) {
	if err := kube.RequireNamespace(
		ctx,
		r.kubeClient,
		session.Spec.SessionNamespace,
	); apierrors.IsNotFound(
		err,
	) {
		return r.store.Delete(ctx, session)
	} else if err != nil {
		return err
	}

	lock, err := kube.AcquireRequiredSessionLock(
		ctx,
		r.store,
		session.Spec.SessionNamespace,
		kube.SessionLockID(session),
	)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		resultErr = errors.Join(resultErr, lock.Release(releaseCtx))
	}()

	latest, err := r.store.GetByKind(
		ctx,
		session.Spec.SessionNamespace,
		session.ID,
		session.BackendResource,
	)
	if kube.IsSessionNotFound(err) {
		return lock.Delete(ctx)
	}

	if err != nil {
		return err
	}

	if latest.BackendUID != session.BackendUID || !latest.Deleting || !latest.PlanPending ||
		latest.Spec.SessionNamespace != session.Spec.SessionNamespace {
		return domain.NewError(
			domain.ErrorConflict,
			"delete workflow",
			"workflow changed while acquiring planning lock",
		)
	}

	// Keep the finalizer as a retry anchor until the planning Lease is gone.
	if err := lock.Delete(ctx); err != nil {
		return err
	}

	return r.store.Delete(ctx, latest)
}
