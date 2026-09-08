package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// workflowInput is an internal projection of operation-specific request fields.
// Admission defines which fields are available on each public CR kind.
type workflowInput struct {
	v1alpha1.TransferOptions `json:",inline"`
	Volumes                  []v1alpha1.VolumeRequest         `json:"volumes"`
	Pod                      *v1alpha1.LocalResourceReference `json:"pod"`
	SourcePVC                *v1alpha1.LocalResourceReference `json:"sourcePVC"`
	SourcePV                 *v1alpha1.LocalResourceReference `json:"sourcePV"`
	DestinationPVC           *v1alpha1.LocalResourceReference `json:"destinationPVC"`
	Online                   bool                             `json:"online"`
	PrecopyPasses            int                              `json:"precopyPasses"`
	SwitchoverCandidate      string                           `json:"switchoverCandidate"`
	AllowLeaderDowntime      bool                             `json:"allowLeaderDowntime"`
	ForceReprovision         bool                             `json:"forceReprovision"`
	OpenEBSLVMEnableShared   bool                             `json:"openebsLvmEnableShared"`
}

// PlanWorkflow resolves user intent without changing data-plane resources.
// The reconciler must persist the returned snapshot before executing it.
func (p *Planner) PlanWorkflow(
	ctx context.Context,
	session *domain.Session,
	image string,
) (domain.SessionSpec, error) {
	var input workflowInput
	if err := json.Unmarshal(session.Intent, &input); err != nil {
		return domain.SessionSpec{}, err
	}

	spec := session.Spec

	operation := spec.Operation()
	if operation == domain.OperationBackup || operation == domain.OperationRestore {
		return p.planRepositoryWorkflow(ctx, session, input, image)
	}

	var (
		plan *domain.MigrationPlan
		err  error
	)
	if operation.RebindsPVC() {
		if input.SourcePVC == nil {
			return spec, domain.NewError(
				domain.ErrorValidation,
				"plan workflow",
				"sourcePVC is required",
			)
		}

		destination := ""
		if input.DestinationPVC != nil {
			destination = input.DestinationPVC.Name
		}

		plan, err = p.planPVCIdentity(
			ctx,
			pvcIdentityPlanOptions{
				Operation:            operation,
				SessionID:            session.ID,
				SourceNamespace:      spec.SourceNamespace,
				SourcePVC:            input.SourcePVC.Name,
				DestinationNamespace: spec.DestinationNamespace,
				DestinationPVC:       destination,
				SessionNamespace:     spec.SessionNamespace,
			},
		)
		input.Volumes = []v1alpha1.VolumeRequest{
			{
				SourcePVC:      *input.SourcePVC,
				SourcePV:       input.SourcePV,
				DestinationPVC: input.DestinationPVC,
			},
		}
	} else {
		plan, err = p.planTransferWorkflow(ctx, session, input, image)
	}

	if err != nil {
		return spec, err
	}

	if !plan.Ready {
		var failures []string
		for _, check := range plan.Checks {
			if !check.Passed {
				failures = append(failures, check.Message)
			}
		}

		return spec, domain.NewError(
			domain.ErrorPrecondition,
			"plan workflow",
			strings.Join(failures, "; "),
		)
	}

	for _, requested := range input.Volumes {
		found := false
		for _, resolved := range plan.SessionSpec.Volumes {
			if resolved.SourcePVC.Name != requested.SourcePVC.Name {
				continue
			}

			found = true

			if err := checkReference(&requested.SourcePVC, resolved.SourcePVC); err != nil {
				return spec, err
			}

			if err := checkReference(requested.SourcePV, resolved.SourcePV); err != nil {
				return spec, err
			}

			if operation != domain.OperationMigratePod {
				if err := checkReference(
					requested.DestinationPVC,
					resolved.DestinationPVC,
				); err != nil {
					return spec, err
				}
			}
		}

		if !found {
			return spec, domain.NewError(
				domain.ErrorPrecondition,
				"plan workflow",
				"requested PVC is not part of the selected workload: "+requested.SourcePVC.Name,
			)
		}
	}

	return plan.SessionSpec, nil
}

func applyWorkflowVolumes(state *planState, input workflowInput) error {
	requests := make(map[string]v1alpha1.VolumeRequest, len(input.Volumes))
	for _, request := range input.Volumes {
		name := request.SourcePVC.Name
		if _, exists := requests[name]; exists {
			return domain.NewError(
				domain.ErrorValidation,
				"plan workflow",
				"duplicate source PVC: "+name,
			)
		}

		requests[name] = request
	}

	for index, name := range state.pvcNames {
		request := requests[name]
		delete(requests, name)

		capacity := input.DestinationCapacity
		if request.Capacity != "" {
			capacity = request.Capacity
		}

		state.requestedCapacities[index] = capacity
		if request.DestinationPVC != nil {
			state.destinationPVCs[index] = request.DestinationPVC.Name
		}

		sourcePath, destinationPath := input.SourcePath, input.DestinationPath
		if request.TransferScope != nil {
			sourcePath = request.TransferScope.SourcePath
			destinationPath = request.TransferScope.DestinationPath
		}

		if sourcePath != "" || destinationPath != "" {
			scope, err := domain.NewTransferScope(sourcePath, destinationPath)
			if err != nil {
				return domain.WrapError(
					domain.ErrorValidation,
					"plan workflow",
					"invalid transfer paths for "+name,
					err,
				)
			}

			state.transferScopes[index] = scope
		}
	}

	for name := range requests {
		return domain.NewError(
			domain.ErrorValidation,
			"plan workflow",
			"PVC override is not part of the selected workload: "+name,
		)
	}

	return nil
}

func checkReference(
	request *v1alpha1.LocalResourceReference,
	resolved domain.ObjectReference,
) error {
	if request == nil {
		return nil
	}

	if request.Name != resolved.Name || (request.UID != "" && request.UID != resolved.UID) ||
		(request.ResourceVersion != "" && request.ResourceVersion != resolved.ResourceVersion) ||
		(request.Kind != "" && request.Kind != resolved.Kind) ||
		(request.APIVersion != "" && request.APIVersion != resolved.APIVersion) {
		return domain.NewError(
			domain.ErrorConflict,
			"plan workflow",
			fmt.Sprintf(
				"%s %s does not match the requested identity constraints",
				resolved.Kind,
				resolved.Name,
			),
		)
	}

	return nil
}

func (p *Planner) planRepositoryWorkflow(
	ctx context.Context,
	session *domain.Session,
	input workflowInput,
	image string,
) (domain.SessionSpec, error) {
	spec := *session.Spec.DeepCopy()
	if spec.Backup != nil {
		pvc, err := p.client.CoreV1().
			PersistentVolumeClaims(spec.SourceNamespace).
			Get(ctx, spec.Backup.SourcePVC.Name, metav1.GetOptions{})
		if err != nil {
			return spec, err
		}

		if pvc.Spec.VolumeName == "" {
			return spec, domain.NewError(
				domain.ErrorPrecondition,
				"plan backup",
				"source PVC must be bound",
			)
		}

		pv, err := p.client.CoreV1().
			PersistentVolumes().
			Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return spec, err
		}

		if err := checkReference(input.SourcePVC, kube.PVCReference(pvc)); err != nil {
			return spec, err
		}

		if err := checkReference(input.SourcePV, kube.PVReference(pv)); err != nil {
			return spec, err
		}

		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID ||
			pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
			pv.Spec.ClaimRef.Name != pvc.Name {
			return spec, domain.NewError(
				domain.ErrorConflict,
				"plan backup",
				"source PV is not bound to the selected PVC identity",
			)
		}

		spec.Backup.SourcePVC = kube.PVCReference(pvc)
		spec.Backup.SourcePV = kube.PVReference(pv)
		spec.Backup.ToolImage = image
	} else if spec.Restore != nil {
		spec.Restore.ToolImage = image

		pvc, err := p.client.CoreV1().
			PersistentVolumeClaims(spec.SourceNamespace).
			Get(ctx, spec.Restore.DestinationPVC.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && spec.Restore.CreatePVC &&
			spec.Restore.DestinationPVC.UID == "" &&
			spec.Restore.DestinationPVC.ResourceVersion == "" {
			return spec, nil
		}

		if err != nil {
			return spec, err
		}

		if err := checkReference(input.DestinationPVC, kube.PVCReference(pvc)); err != nil {
			return spec, err
		}

		spec.Restore.DestinationPVC = kube.PVCReference(pvc)
	}

	return spec, nil
}

func (p *Planner) planTransferWorkflow(
	ctx context.Context,
	session *domain.Session,
	input workflowInput,
	image string,
) (*domain.MigrationPlan, error) {
	spec := session.Spec
	operation := spec.Operation()

	options := planOptions{
		Operation:              operation,
		SessionID:              session.ID,
		SourceNamespace:        spec.SourceNamespace,
		TemporaryNamespace:     spec.TemporaryNamespace,
		DestinationNamespace:   spec.DestinationNamespace,
		SessionNamespace:       spec.SessionNamespace,
		StagingNamespace:       spec.TemporaryNamespace,
		ToolImage:              image,
		DestinationClass:       input.DestinationStorageClass,
		SourceNode:             input.SourceNode,
		TargetNode:             input.TargetNode,
		CapacityAwareness:      domain.CapacityAwareness(input.CapacityAwareness),
		Strategies:             input.Strategies,
		VerifyChecksum:         input.VerifyChecksum,
		DeleteExtraneous:       input.DeleteExtraneous,
		AllowVolumeShrink:      input.AllowVolumeShrink,
		SkipSourceUsageCheck:   input.SkipSourceUsageCheck,
		Online:                 input.Online,
		PrecopyPasses:          input.PrecopyPasses,
		SwitchoverCandidate:    input.SwitchoverCandidate,
		AllowLeaderDowntime:    input.AllowLeaderDowntime,
		ForceReprovision:       input.ForceReprovision,
		OpenEBSLVMEnableShared: input.OpenEBSLVMEnableShared,
	}
	if input.Pod != nil {
		options.PodName = input.Pod.Name
	}

	for _, volume := range input.Volumes {
		name := volume.SourcePVC.Name
		if operation == domain.OperationMigratePod && volume.DestinationPVC != nil {
			return nil, domain.NewError(
				domain.ErrorValidation,
				"plan workflow",
				"Pod migration preserves PVC names; destinationPVC is not supported",
			)
		}

		if input.Pod == nil {
			options.SourcePVCs = append(options.SourcePVCs, name)
		}

		if volume.DestinationPVC != nil {
			options.DestinationPVCs = append(
				options.DestinationPVCs,
				name+"="+volume.DestinationPVC.Name,
			)
		}

		if volume.Capacity != "" {
			options.DestinationCapacities = append(
				options.DestinationCapacities,
				name+"="+volume.Capacity,
			)
		}

		if volume.TransferScope != nil {
			options.SourcePaths = append(
				options.SourcePaths,
				name+"="+volume.TransferScope.SourcePath,
			)
			options.DestinationPaths = append(
				options.DestinationPaths,
				name+"="+volume.TransferScope.DestinationPath,
			)
		}
	}

	if input.DestinationCapacity != "" {
		options.DestinationCapacities = append(
			options.DestinationCapacities,
			input.DestinationCapacity,
		)
	}

	state := newPlanState(p, options)
	p.validatePlanInputs(state.plan, state.options)

	if operation == domain.OperationMigrate {
		p.prepareOfflineMigration(&state)
	} else if err := p.discoverPlanWorkload(ctx, &state); err != nil {
		return nil, err
	}

	if input.Pod != nil && state.sourcePod != nil {
		if err := checkReference(input.Pod, kube.PodReference(state.sourcePod)); err != nil {
			return nil, err
		}
	}
	// Public requests allow partial overrides; the CLI mapping syntax retains
	// its stricter completeness checks for local previews and session mode.
	state.options.DestinationPVCs = nil
	state.options.DestinationCapacities = nil
	state.options.SourcePaths = nil
	state.options.DestinationPaths = nil
	p.loadPlanContext(ctx, &state)

	if err := applyWorkflowVolumes(&state, input); err != nil {
		return nil, err
	}

	p.planVolumes(ctx, &state)
	p.finalizePlan(ctx, &state)

	return state.plan, nil
}
