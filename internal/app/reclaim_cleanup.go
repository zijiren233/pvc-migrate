package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type reclaimVolume struct {
	uncheckpointed *domain.ObjectReference
	skipMissingPV  bool
	role           string
	pvc            domain.ObjectReference
	pv             domain.ObjectReference
	policy         corev1.PersistentVolumeReclaimPolicy
	metadata       domain.PVCMetadata
	delete         bool
}

func (s *Service) skipMissingReclaimPV(ctx context.Context, v reclaimVolume) (bool, error) {
	if !v.skipMissingPV {
		return false, nil
	}

	_, err := s.client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	return false, err
}

func reclaimPolicies(session *domain.Session, options CleanupOptions) (string, string, error) {
	source, destination := session.Spec.SourcePVReclaimPolicy, session.Spec.DestinationPVCReclaimPolicy
	if options.SourcePVReclaimPolicy != "" {
		source = options.SourcePVReclaimPolicy
	}

	if options.DestinationPVCReclaimPolicy != "" {
		destination = options.DestinationPVCReclaimPolicy
	}

	for _, policy := range []string{source, destination} {
		if policy != "" && policy != "Retain" && policy != "Delete" {
			return "", "", domain.NewError(
				domain.ErrorValidation,
				"cleanup",
				"reclaim policy must be Retain or Delete",
			)
		}
	}

	if source == "Delete" &&
		(cleanupKeepsSource(session) || session.Status.Phase != domain.PhaseCompleted) {
		if options.SourcePVReclaimPolicy == "Delete" {
			return "", "", domain.NewError(
				domain.ErrorPrecondition,
				"cleanup",
				"source PV is still active; only a completed migration has an old source PV to delete",
			)
		}

		source = "Retain"
	}

	return source, destination, nil
}

// Resolve identities from the completed checkpoint: cutover replaces the
// staging PVC, while rollback makes the original source PV active again.
func reclaimVolumes(session *domain.Session, options CleanupOptions) ([]reclaimVolume, error) {
	sourcePolicy, destinationPolicy, err := reclaimPolicies(session, options)
	if err != nil {
		return nil, err
	}

	volumes := make([]reclaimVolume, 0, len(session.Spec.Volumes)*2)
	for index, v := range session.Spec.Volumes {
		completed := session.Status.Phase == domain.PhaseCompleted && !cleanupKeepsSource(session)
		src := reclaimVolume{
			skipMissingPV: session.Status.Phase == domain.PhaseAborted,
			pv:            v.SourcePV,
			policy:        v.SourceReclaimPolicy,
			metadata:      v.SourcePVCMetadata,
		}

		dst := reclaimVolume{
			role:   kube.ResourceRoleDestination,
			pvc:    v.DestinationPVC,
			pv:     v.DestinationPV,
			policy: v.DestinationPolicy,
		}
		if uncheckpointedDestination(session, index) {
			ref := v.DestinationPVC
			dst.uncheckpointed = &ref
		}

		if completed {
			src.role = kube.ResourceRoleRollback
			dst.role = kube.ResourceRoleActive
			src.delete = sourcePolicy == "Delete"

			if !src.delete && src.policy != "" {
				src.policy = corev1.PersistentVolumeReclaimRetain
			}

			dst.pvc = session.Status.Volumes[index].Activation.ActivePVC
			dst.metadata = v.SourcePVCMetadata
		} else {
			if session.Status.Phase == domain.PhaseRolledBack {
				dst.role = kube.ResourceRoleRollback
			}

			src.pvc = session.Status.Volumes[index].Activation.ActivePVC
			if src.pvc.Name == "" {
				src.pvc = v.SourcePVC
			}
		}

		dst.delete = destinationPolicy == "Delete"
		if !completed && !dst.delete && dst.pvc.UID == "" && dst.policy != "" {
			dst.policy = corev1.PersistentVolumeReclaimRetain
		}

		if !uncheckpointedSource(session, index) {
			volumes = append(volumes, src)
		}

		if dst.pv.Name != v.SourcePV.Name && (dst.pvc.UID == "" || dst.pvc.UID != v.SourcePVC.UID) {
			volumes = append(volumes, dst)
		}
	}

	return volumes, nil
}

func (s *Service) cleanupByReclaimPolicy(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	dryRun bool,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup", "session is nil")
	}

	if session.Spec.Operation().RebindsPVC() || session.Spec.Type == domain.SessionTypeBackup ||
		session.Spec.Type == domain.SessionTypeRestore {
		return s.cleanupIdentity(ctx, session, options, dryRun)
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if session.PlanPending {
		if options.DeleteSession && !dryRun {
			return s.deleteCleanupSession(ctx, session)
		}
		return nil
	}

	if !cleanupPhaseAllowed(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			fmt.Sprintf("session phase %s is still active", session.Status.Phase),
		)
	}

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			"deleting the session requires --finalize",
		)
	}
	// Discovery in a preview must never checkpoint or modify the caller's plan.
	preview := *session

	preview.Spec = *session.Spec.DeepCopy()
	for index := range preview.Spec.Volumes {
		if _, err := s.discoverDestinationRefs(ctx, &preview, index); err != nil {
			return err
		}
	}

	volumes, err := reclaimVolumes(&preview, options)
	if err != nil {
		return err
	}

	if err := s.protectRetainedPVs(ctx, volumes); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if err := s.validateReservationPods(ctx, session); err != nil {
		return err
	}

	if err := s.validateAbortedSources(ctx, session); err != nil {
		return err
	}

	if options.Finalize {
		if err := s.validateStandalonePodOwnershipRelease(ctx, session); err != nil {
			return err
		}
	}

	for _, v := range volumes {
		if err := s.validateReclaimVolume(ctx, session, v, options.Finalize); err != nil {
			return err
		}

		s.logInfo(
			"cleanup volume policy",
			"pvc",
			v.pvc.Name,
			"pv",
			v.pv.Name,
			"delete",
			v.delete,
			"dryRun",
			dryRun,
		)
	}

	if dryRun {
		return nil
	}

	return s.executeReclaimCleanup(ctx, session, options, volumes)
}

func (s *Service) protectRetainedPVs(ctx context.Context, volumes []reclaimVolume) error {
	for i := range volumes {
		v := &volumes[i]
		if v.delete || v.pv.Name == "" || v.policy == "" {
			continue
		}
		// A released PV must stay Retain after ownership is removed, even when
		// its original StorageClass policy was Delete and its PVC UID is recorded.
		if v.pvc.UID == "" {
			v.policy = corev1.PersistentVolumeReclaimRetain
			continue
		}

		pvc, err := s.client.CoreV1().
			PersistentVolumeClaims(v.pvc.Namespace).
			Get(ctx, v.pvc.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || (err == nil && pvc.DeletionTimestamp != nil) {
			v.policy = corev1.PersistentVolumeReclaimRetain
		} else if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) executeReclaimCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	volumes []reclaimVolume,
) error {
	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.recoverDestinationRefs(ctx, session); err != nil {
		return err
	}

	if err := s.deleteReservationPods(ctx, session); err != nil {
		return err
	}

	if err := s.releaseAbortedSources(ctx, session); err != nil {
		return err
	}

	for _, v := range volumes {
		if err := s.reclaimVolume(ctx, session, v, options.Finalize); err != nil {
			return err
		}
	}

	if options.Finalize {
		if err := s.releaseStandalonePodOwnership(ctx, session); err != nil {
			return err
		}
	}

	if options.DeleteSession {
		return s.deleteCleanupSession(ctx, session)
	}

	return nil
}

func (s *Service) validateReclaimVolume(
	ctx context.Context,
	session *domain.Session,
	v reclaimVolume,
	finalize bool,
) error {
	if skip, err := s.skipMissingReclaimPV(ctx, v); skip || err != nil {
		return err
	}

	if !v.delete && !finalize {
		return nil
	}

	if err := s.validateReclaimPVC(ctx, session, v); err != nil {
		return err
	}

	return s.validateReclaimPV(ctx, session, v)
}

func (s *Service) validateReclaimPVC(
	ctx context.Context,
	session *domain.Session,
	v reclaimVolume,
) error {
	if v.pvc.UID != "" {
		pvc, err := s.client.CoreV1().
			PersistentVolumeClaims(v.pvc.Namespace).
			Get(ctx, v.pvc.Name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		if err == nil {
			if pvc.UID != v.pvc.UID ||
				(pvc.Labels[kube.SessionKey] != "" && pvc.Labels[kube.SessionKey] != session.ID) ||
				(pvc.Annotations[kube.SessionKey] != "" && pvc.Annotations[kube.SessionKey] != session.ID) {
				return domain.NewError(
					domain.ErrorConflict,
					"cleanup",
					"PVC identity or ownership changed",
				)
			}

			if v.delete {
				if pvc.Labels[kube.SessionKey] != session.ID {
					return domain.NewError(
						domain.ErrorConflict,
						"cleanup",
						"PVC is no longer owned by this workflow",
					)
				}

				current, err := s.inspectPVCUnusedForSession(ctx, v.pvc, session)
				if err != nil {
					return err
				}

				if current != nil && current.UID != v.pvc.UID {
					return domain.NewError(
						domain.ErrorConflict,
						"cleanup",
						"PVC identity changed during consumer checks",
					)
				}
			}
		}
	}

	return nil
}

func (s *Service) validateReclaimPV(
	ctx context.Context,
	session *domain.Session,
	v reclaimVolume,
) error {
	if v.pv.Name == "" {
		return nil
	}

	if v.policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			"PV "+v.pv.Name+" has no recorded reclaim policy",
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	uncheckpointed := v.uncheckpointed != nil &&
		uncheckpointedDestinationPVMatches(pv, *v.uncheckpointed) &&
		pv.UID == v.pv.UID
	if err := validateFinalizablePV(pv, v.pv, session.ID, v.policy); err != nil && !uncheckpointed {
		return err
	}

	if v.role != "" && pv.Labels[kube.SessionKey] == session.ID &&
		pv.Labels[kube.ResourceRoleLabel] != v.role {
		return domain.NewError(domain.ErrorConflict, "cleanup", "PV "+v.pv.Name+" role changed")
	}

	if v.delete && pv.Labels[kube.SessionKey] != session.ID && !uncheckpointed {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			"PV is no longer owned by this workflow",
		)
	}

	if pv.Spec.ClaimRef != nil && pv.Status.Phase == corev1.VolumeBound {
		if v.pvc.UID == "" || pv.Spec.ClaimRef.UID != v.pvc.UID ||
			pv.Spec.ClaimRef.Name != v.pvc.Name ||
			pv.Spec.ClaimRef.Namespace != v.pvc.Namespace {
			return domain.NewError(
				domain.ErrorConflict,
				"cleanup",
				"PV binding changed; refusing to reclaim another claim's storage",
			)
		}
	}

	return nil
}

func (s *Service) reclaimVolume(
	ctx context.Context,
	session *domain.Session,
	v reclaimVolume,
	finalize bool,
) error {
	if skip, err := s.skipMissingReclaimPV(ctx, v); skip || err != nil {
		return err
	}

	if !v.delete && !finalize {
		return nil
	}

	if err := s.validateReclaimVolume(ctx, session, v, finalize); err != nil {
		return err
	}

	if v.delete {
		if v.pvc.UID != "" {
			if err := s.deleteManagedPVC(ctx, session.ID, v.pvc); err != nil {
				return err
			}
		}

		if v.pv.Name == "" {
			return nil
		}

		_, err := s.client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		return s.deleteReclaimedPV(
			ctx,
			session.ID,
			v.pv,
			v.role,
			v.policy,
			v.uncheckpointed,
		)
	}

	if v.pvc.UID != "" {
		if err := kube.FinalizePVC(ctx, s.client, v.pvc, session.ID, v.metadata); err != nil {
			return err
		}
	}

	if v.pv.Name == "" {
		return nil
	}

	_, err := s.client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	return s.finalizeActivePV(ctx, session.ID, v.pv, v.policy)
}
