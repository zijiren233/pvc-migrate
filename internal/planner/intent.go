package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// ForSubmission keeps previews local while delegating actual controller work.
func (p *Planner) ForSubmission(requestOnly bool) *Planner {
	clone := *p
	clone.requestOnly = requestOnly
	return &clone
}

func (p *Planner) intentPlan(options planOptions) (*domain.MigrationPlan, error) {
	if err := domain.ValidateReclaimPolicies(
		options.SourcePVReclaimPolicy,
		options.DestinationPVCReclaimPolicy,
	); err != nil {
		return nil, err
	}

	body := map[string]any{
		"sourcePVReclaimPolicy":       options.SourcePVReclaimPolicy,
		"destinationPVCReclaimPolicy": options.DestinationPVCReclaimPolicy,
		"sourceNamespace":             options.SourceNamespace,
		"destinationNamespace":        options.DestinationNamespace,
		"temporaryNamespace":          options.TemporaryNamespace,
		"sessionNamespace":            options.SessionNamespace,
		"destinationStorageClass":     options.DestinationClass,
		"sourceNode":                  options.SourceNode,
		"targetNode":                  options.TargetNode,
		"capacityAwareness":           options.CapacityAwareness,
		"strategies":                  options.Strategies,
		"verifyChecksum":              options.VerifyChecksum,
		"deleteExtraneous":            options.DeleteExtraneous,
		"allowVolumeShrink":           options.AllowVolumeShrink,
		"skipSourceUsageCheck":        options.SkipSourceUsageCheck,
		"online":                      options.Online,
		"precopyPasses":               options.PrecopyPasses,
		"switchoverCandidate":         options.SwitchoverCandidate,
		"allowLeaderDowntime":         options.AllowLeaderDowntime,
		"forceReprovision":            options.ForceReprovision,
		"openebsLvmEnableShared":      options.OpenEBSLVMEnableShared,
	}
	if options.PodName != "" {
		body["pod"] = v1alpha1.LocalResourceReference{Name: options.PodName}
	}

	volumes := make([]v1alpha1.VolumeRequest, 0, len(options.SourcePVCs))
	for _, name := range options.SourcePVCs {
		volumes = append(
			volumes,
			v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}},
		)
	}

	for _, field := range []struct {
		key    string
		values []string
	}{{"capacity", options.DestinationCapacities}, {"destinationPVC", options.DestinationPVCs}, {"sourcePath", options.SourcePaths}, {"destinationPath", options.DestinationPaths}} {
		seen := make(map[string]bool)
		for index, raw := range field.values {
			name, value, named := strings.Cut(raw, "=")
			if strings.TrimSpace(raw) == "" || (named && (name == "" || value == "")) {
				return nil, fmt.Errorf("invalid %s mapping %q", field.key, raw)
			}

			if !named {
				value = raw
				if field.key != "destinationPVC" {
					if len(field.values) != 1 {
						return nil, fmt.Errorf(
							"%s requires one broadcast value or named PVC mappings",
							field.key,
						)
					}

					key := field.key
					if key == "capacity" {
						key = "destinationCapacity"
					}

					body[key] = value

					continue
				}

				if index >= len(volumes) {
					return nil, errors.New("destination PVC requires a source PVC mapping")
				}

				name = volumes[index].SourcePVC.Name
			}

			if seen[name] {
				return nil, fmt.Errorf("duplicate %s mapping for PVC %q", field.key, name)
			}

			seen[name] = true

			found := -1
			for i := range volumes {
				if volumes[i].SourcePVC.Name == name {
					found = i
					break
				}
			}

			if found < 0 {
				if options.PodName == "" {
					return nil, fmt.Errorf("unknown source PVC %q", name)
				}

				volumes = append(
					volumes,
					v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}},
				)
				found = len(volumes) - 1
			}

			setVolumeRequestField(&volumes[found], field.key, value)
		}
	}

	if len(volumes) > 0 {
		body["volumes"] = volumes
	}

	kind := domain.ControllerKindCopy
	switch options.Operation {
	case domain.OperationMigrate:
		kind = domain.ControllerKindMigration
	case domain.OperationMigratePod:
		kind = domain.ControllerKindPodMigration
	case domain.OperationReserve:
		kind = domain.ControllerKindReservation
	}

	if options.SourceNamespace != options.SessionNamespace ||
		options.SourceNamespace != options.TemporaryNamespace ||
		options.SourceNamespace != options.DestinationNamespace {
		kind = domain.ControllerKind("Cluster" + string(kind))
	}

	return intentPlanForKind(options.SessionID, options.SourceNamespace, kind, body)
}

func setVolumeRequestField(v *v1alpha1.VolumeRequest, field, value string) {
	switch field {
	case "capacity":
		v.Capacity = value
	case "destinationPVC":
		v.DestinationPVC = &v1alpha1.LocalResourceReference{Name: value}
	case "sourcePath", "destinationPath":
		if v.TransferScope == nil {
			v.TransferScope = &v1alpha1.TransferScope{
				SourcePath: domain.VolumeRootPath, DestinationPath: domain.VolumeRootPath,
			}
		}

		if field == "sourcePath" {
			v.TransferScope.SourcePath = value
		} else {
			v.TransferScope.DestinationPath = value
		}
	}
}

func intentPlanForKind(
	id, namespace string,
	kind domain.ControllerKind,
	body any,
) (*domain.MigrationPlan, error) {
	object := kube.WorkflowObjectForKind(kind)
	object.SetName(id)

	if !domain.IsClusterControllerKind(kind) {
		object.SetNamespace(namespace)
	}

	data, err := json.Marshal(struct {
		Spec any `json:"spec"`
	}{body})
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, object); err != nil {
		return nil, err
	}

	object.GetObjectKind().SetGroupVersionKind(v1alpha1.GroupVersion.WithKind(string(kind)))

	session, err := kube.DecodeWorkflow(object)
	if err != nil {
		return nil, err
	}

	return &domain.MigrationPlan{
		APIVersion:           domain.SessionAPIVersion,
		Kind:                 domain.MigrationPlanKind,
		SessionID:            id,
		SourceNamespace:      session.Spec.SourceNamespace,
		TemporaryNamespace:   session.Spec.TemporaryNamespace,
		DestinationNamespace: session.Spec.DestinationNamespace,
		SessionNamespace:     session.Spec.SessionNamespace,
		SessionSpec:          session.Spec,
		Intent:               session.Intent,
		Ready:                true,
	}, nil
}

func (p *Planner) identityIntentPlan(
	options pvcIdentityPlanOptions,
) (*domain.MigrationPlan, error) {
	kind := domain.ControllerKindRename
	if options.Operation == domain.OperationMove {
		kind = domain.ControllerKindMove
	}

	body := map[string]any{
		"sourcePVC":            v1alpha1.LocalResourceReference{Name: options.SourcePVC},
		"destinationPVC":       v1alpha1.LocalResourceReference{Name: options.DestinationPVC},
		"sourceNamespace":      options.SourceNamespace,
		"destinationNamespace": options.DestinationNamespace,
		"sessionNamespace":     options.SessionNamespace,
	}

	return intentPlanForKind(options.SessionID, options.SourceNamespace, kind, body)
}
