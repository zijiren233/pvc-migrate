package planner

import (
	"context"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type OfflineMigrationOptions struct {
	SessionID                   string
	SourceNamespace             string
	TemporaryNamespace          string
	DestinationNamespace        string
	SessionNamespace            string
	StagingNamespace            string
	ToolImage                   string
	CapacityAwareness           domain.CapacityAwareness
	SourcePVCs                  []string
	DestinationPVCs             []string
	DestinationCapacities       []string
	SourcePaths                 []string
	DestinationPaths            []string
	AllowVolumeShrink           bool
	SkipSourceUsageCheck        bool
	SourceNode                  string
	TargetNode                  string
	DestinationClass            string
	Strategies                  []string
	VerifyChecksum              bool
	DeleteExtraneous            bool
	SourcePVReclaimPolicy       string
	DestinationPVCReclaimPolicy string
}

func (p *Planner) PlanOfflineMigration(
	ctx context.Context,
	options OfflineMigrationOptions,
) (*domain.MigrationPlan, error) {
	if options.SourcePVReclaimPolicy != "" &&
		options.SourcePVReclaimPolicy != domain.SourcePVReclaimRetain &&
		options.SourcePVReclaimPolicy != domain.SourcePVReclaimDelete {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan migration",
			"source-pv-reclaim-policy must be Retain or Delete",
		)
	}

	if options.DestinationPVCReclaimPolicy != "" &&
		options.DestinationPVCReclaimPolicy != domain.DestinationPVCReclaimRetain &&
		options.DestinationPVCReclaimPolicy != domain.DestinationPVCReclaimDelete {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan migration",
			"destination-pvc-reclaim-policy must be Retain or Delete",
		)
	}

	state := newPlanState(p, planOptions{
		SessionID:                   options.SessionID,
		Operation:                   domain.OperationMigrate,
		SourceNamespace:             options.SourceNamespace,
		TemporaryNamespace:          options.TemporaryNamespace,
		DestinationNamespace:        options.DestinationNamespace,
		SessionNamespace:            options.SessionNamespace,
		StagingNamespace:            options.StagingNamespace,
		ToolImage:                   options.ToolImage,
		CapacityAwareness:           options.CapacityAwareness,
		SourcePVCs:                  slices.Clone(options.SourcePVCs),
		DestinationPVCs:             slices.Clone(options.DestinationPVCs),
		DestinationCapacities:       slices.Clone(options.DestinationCapacities),
		SourcePaths:                 slices.Clone(options.SourcePaths),
		DestinationPaths:            slices.Clone(options.DestinationPaths),
		AllowVolumeShrink:           options.AllowVolumeShrink,
		SkipSourceUsageCheck:        options.SkipSourceUsageCheck,
		SourceNode:                  options.SourceNode,
		TargetNode:                  options.TargetNode,
		DestinationClass:            options.DestinationClass,
		Strategies:                  slices.Clone(options.Strategies),
		VerifyChecksum:              options.VerifyChecksum,
		DeleteExtraneous:            options.DeleteExtraneous,
		SourcePVReclaimPolicy:       options.SourcePVReclaimPolicy,
		DestinationPVCReclaimPolicy: options.DestinationPVCReclaimPolicy,
	})
	if p.requestOnly {
		return p.intentPlan(state.options)
	}

	p.validatePlanInputs(state.plan, state.options)
	p.prepareOfflineMigration(&state)

	return p.completePlan(ctx, &state), nil
}
