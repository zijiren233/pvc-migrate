package planner

import (
	"context"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type PodMigrationOptions struct {
	SessionID                   string
	SourceNamespace             string
	TemporaryNamespace          string
	SessionNamespace            string
	StagingNamespace            string
	ToolImage                   string
	CapacityAwareness           domain.CapacityAwareness
	DestinationCapacities       []string
	SourcePaths                 []string
	DestinationPaths            []string
	AllowVolumeShrink           bool
	SkipSourceUsageCheck        bool
	PodName                     string
	SourceNode                  string
	TargetNode                  string
	DestinationClass            string
	Strategies                  []string
	VerifyChecksum              bool
	DeleteExtraneous            bool
	SwitchoverCandidate         string
	AllowLeaderDowntime         bool
	ForceReprovision            bool
	PrecopyPasses               int
	OpenEBSLVMEnableShared      bool
	SourcePVReclaimPolicy       string
	DestinationPVCReclaimPolicy string
}

func (p *Planner) PlanPodMigration(
	ctx context.Context,
	options PodMigrationOptions,
) (*domain.MigrationPlan, error) {
	if options.SourcePVReclaimPolicy != "" &&
		options.SourcePVReclaimPolicy != domain.SourcePVReclaimRetain &&
		options.SourcePVReclaimPolicy != domain.SourcePVReclaimDelete {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan pod migration",
			"source-pv-reclaim-policy must be Retain or Delete",
		)
	}

	if options.DestinationPVCReclaimPolicy != "" &&
		options.DestinationPVCReclaimPolicy != domain.DestinationPVCReclaimRetain &&
		options.DestinationPVCReclaimPolicy != domain.DestinationPVCReclaimDelete {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan pod migration",
			"destination-pvc-reclaim-policy must be Retain or Delete",
		)
	}

	state := newPlanState(p, planOptions{
		SessionID:                   options.SessionID,
		Operation:                   domain.OperationMigratePod,
		SourceNamespace:             options.SourceNamespace,
		TemporaryNamespace:          options.TemporaryNamespace,
		DestinationNamespace:        options.SourceNamespace,
		SessionNamespace:            options.SessionNamespace,
		StagingNamespace:            options.StagingNamespace,
		ToolImage:                   options.ToolImage,
		CapacityAwareness:           options.CapacityAwareness,
		DestinationCapacities:       slices.Clone(options.DestinationCapacities),
		SourcePaths:                 slices.Clone(options.SourcePaths),
		DestinationPaths:            slices.Clone(options.DestinationPaths),
		AllowVolumeShrink:           options.AllowVolumeShrink,
		SkipSourceUsageCheck:        options.SkipSourceUsageCheck,
		PodName:                     options.PodName,
		SourceNode:                  options.SourceNode,
		TargetNode:                  options.TargetNode,
		DestinationClass:            options.DestinationClass,
		Strategies:                  slices.Clone(options.Strategies),
		VerifyChecksum:              options.VerifyChecksum,
		DeleteExtraneous:            options.DeleteExtraneous,
		SwitchoverCandidate:         options.SwitchoverCandidate,
		AllowLeaderDowntime:         options.AllowLeaderDowntime,
		ForceReprovision:            options.ForceReprovision,
		PrecopyPasses:               options.PrecopyPasses,
		OpenEBSLVMEnableShared:      options.OpenEBSLVMEnableShared,
		SourcePVReclaimPolicy:       options.SourcePVReclaimPolicy,
		DestinationPVCReclaimPolicy: options.DestinationPVCReclaimPolicy,
	})
	if p.requestOnly {
		return p.intentPlan(state.options)
	}

	p.validatePlanInputs(state.plan, state.options)

	if err := p.discoverPlanWorkload(ctx, &state); err != nil {
		return nil, err
	}

	return p.completePlan(ctx, &state), nil
}
