package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

// Cleanup validation mirrors destructive cleanup guards without mutating
// Kubernetes resources.
func (s *Service) validateCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupByReclaimPolicy(ctx, session, options, true)
}

func validateFinalizablePV(
	pv *corev1.PersistentVolume,
	ref domain.ObjectReference,
	sessionID string,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	role := pv.Labels[kube.ResourceRoleLabel]
	if pv.Labels[kube.SessionKey] == "" && role == "" &&
		pv.Spec.PersistentVolumeReclaimPolicy == policy &&
		pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
		return nil
	}

	if pv.Labels[kube.SessionKey] != sessionID ||
		(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination && role != kube.ResourceRoleRollback) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return nil
}
