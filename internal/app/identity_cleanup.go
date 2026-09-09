package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (s *Service) cleanupIdentity(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	dryRun bool,
) error {
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

	if options.SourcePVReclaimPolicy != "" || options.DestinationPVCReclaimPolicy != "" {
		return domain.NewError(
			domain.ErrorValidation,
			"cleanup",
			"this workflow does not create independent source and destination volumes",
		)
	}

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			"deleting the session requires --finalize",
		)
	}

	if session.Spec.Type == domain.SessionTypeBackup ||
		session.Spec.Type == domain.SessionTypeRestore {
		if options.Finalize || options.DeleteSession {
			if err := kube.ValidateBackupCredentialsSecretCleanup(
				ctx,
				s.client,
				backupCredentialsCleanupReference(session),
				session.ID,
			); err != nil {
				return err
			}

			if !dryRun {
				if err := s.cleanupBackupCredentials(ctx, session); err != nil {
					return err
				}
			}
		}
	} else if options.Finalize {
		for i, v := range session.Spec.Volumes {
			item := reclaimVolume{
				pv:       v.SourcePV,
				pvc:      session.Status.Volumes[i].Activation.ActivePVC,
				policy:   v.SourceReclaimPolicy,
				metadata: v.SourcePVCMetadata,
			}
			if item.pvc.Name == "" {
				item.pvc = v.SourcePVC
			}

			if err := s.validateReclaimVolume(ctx, session, item, true); err != nil {
				return err
			}

			if !dryRun {
				if err := s.reclaimVolume(ctx, session, item, true); err != nil {
					return err
				}
			}
		}
	}

	if !dryRun && options.DeleteSession {
		return s.deleteCleanupSession(ctx, session)
	}

	return nil
}
