package app

import "github.com/labring-sigs/pvc-migrate/internal/domain"

func cleanupKeepsSource(session *domain.Session) bool {
	return session != nil &&
		(session.Spec.Operation() == domain.OperationCopy || session.Spec.Operation() == domain.OperationReserve)
}

func cleanupPhaseAllowed(session *domain.Session) bool {
	if session == nil {
		return false
	}

	if session.Status.Phase == domain.PhaseAborted {
		return true
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve:
		return session.Status.Phase == domain.PhaseReserved
	case domain.OperationCopy:
		return session.Status.Phase == domain.PhaseWarmCopied
	default:
		return session.Status.Phase == domain.PhaseCompleted ||
			session.Status.Phase == domain.PhaseRolledBack
	}
}
