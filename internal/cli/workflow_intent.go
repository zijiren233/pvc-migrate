package cli

import (
	"context"
	"encoding/json"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
)

func (r *rootState) submitRepositoryIntent(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	flags *bucketFlags,
	spec any,
	kind domain.ControllerKind,
) error {
	if flags.backupRepositoryNamespace != "" && flags.backupRepositoryNamespace != flags.namespace {
		return domain.NewError(
			domain.ErrorValidation,
			"submit workflow",
			"BackupRepository must be in the workflow namespace",
		)
	}

	if flags.id == "" {
		id, err := domain.NewSessionID(time.Now())
		if err != nil {
			return err
		}

		flags.id = id
	}

	object := kube.WorkflowObjectForKind(kind)
	object.SetName(flags.id)
	object.SetNamespace(flags.namespace)

	data, err := json.Marshal(struct {
		Spec any `json:"spec"`
	}{Spec: spec})
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, object); err != nil {
		return err
	}

	request, err := kube.DecodeWorkflow(object)
	if err != nil {
		return err
	}

	if err := r.confirm(ctx, cmd, flags.name); err != nil {
		return reportApprovalError(cmd, err)
	}

	plan := &domain.MigrationPlan{
		SessionID:        request.ID,
		SessionNamespace: request.Spec.SessionNamespace,
		SessionSpec:      request.Spec,
		Intent:           request.Intent,
		Ready:            true,
	}

	session, err := runtime.service.CreateSession(ctx, plan, false)
	if err != nil {
		return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
	}

	_, err = deferControllerExecution(ctx, cmd, runtime, session)

	return err
}
