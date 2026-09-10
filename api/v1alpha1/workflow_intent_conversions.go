package v1alpha1

import (
	"encoding/json"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// projectFields copies the shared, typed request fields without exposing snapshots.
func projectFields[T any](in any) T {
	data, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}

	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}

	return out
}

func requestedVolumes(volumes []domain.VolumeSpec) []VolumeRequest {
	out := make([]VolumeRequest, 0, len(volumes))
	for _, v := range volumes {
		dst := plannedDestinationRefFromDomain(v.DestinationPVC)
		pv := localRefFromDomain(v.SourcePV)
		out = append(
			out,
			VolumeRequest{
				SourcePVC:      localRefFromDomain(v.SourcePVC),
				SourcePV:       &pv,
				DestinationPVC: &dst,
				Capacity:       v.Capacity,
				TransferScope:  scopeFromDomain(v.TransferScope),
			},
		)
	}

	return out
}

func MigrationSpecFromDomain(spec domain.SessionSpec) MigrationSpec {
	out := projectFields[MigrationSpec](MigrationPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	return out
}

func (s MigrationSpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[MigrationPlan](s)
	return plan.Domain(namespace)
}

func PodMigrationSpecFromDomain(spec domain.SessionSpec) PodMigrationSpec {
	out := projectFields[PodMigrationSpec](PodMigrationPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	out.Pod = localRefFromDomain(spec.Workload().Pod)
	for i := range out.Volumes {
		out.Volumes[i].DestinationPVC = nil
	}

	return out
}

func (s PodMigrationSpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[PodMigrationPlan](s)
	return plan.Domain(namespace)
}

func ReservationSpecFromDomain(spec domain.SessionSpec) ReservationSpec {
	out := projectFields[ReservationSpec](ReservationPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	return out
}

func (s ReservationSpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[ReservationPlan](s)
	return plan.Domain(namespace)
}

func CopySpecFromDomain(spec domain.SessionSpec) CopySpec {
	out := projectFields[CopySpec](CopyPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	return out
}

func (s CopySpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[CopyPlan](s)
	return plan.Domain(namespace)
}

func BackupSpecFromDomain(spec domain.SessionSpec) BackupSpec {
	out := projectFields[BackupSpec](BackupPlanFromDomain(spec))
	return out
}

func (s BackupSpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[BackupPlan](s)
	return plan.Domain(namespace)
}

func RestoreSpecFromDomain(spec domain.SessionSpec) RestoreSpec {
	out := projectFields[RestoreSpec](RestorePlanFromDomain(spec))
	return out
}

func (s RestoreSpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[RestorePlan](s)
	return plan.Domain(namespace)
}

func RenameSpecFromDomain(spec domain.SessionSpec) RenameSpec {
	out := projectFields[RenameSpec](RenamePlanFromDomain(spec))
	return out
}

func (s RenameSpec) Domain(namespace string) domain.SessionSpec {
	plan := projectFields[RenamePlan](s)
	return plan.Domain(namespace)
}

func ClusterMigrationSpecFromDomain(spec domain.SessionSpec) ClusterMigrationSpec {
	out := projectFields[ClusterMigrationSpec](ClusterMigrationPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	return out
}

func (s ClusterMigrationSpec) Domain() domain.SessionSpec {
	plan := projectFields[ClusterMigrationPlan](s)
	if plan.SessionNamespace == "" {
		plan.SessionNamespace = plan.SourceNamespace
	}

	if plan.TemporaryNamespace == "" {
		plan.TemporaryNamespace = plan.SourceNamespace
	}

	return plan.Domain()
}

func ClusterPodMigrationSpecFromDomain(spec domain.SessionSpec) ClusterPodMigrationSpec {
	out := projectFields[ClusterPodMigrationSpec](ClusterPodMigrationPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	out.Pod = localRefFromDomain(spec.Workload().Pod)
	for i := range out.Volumes {
		out.Volumes[i].DestinationPVC = nil
	}

	return out
}

func (s ClusterPodMigrationSpec) Domain() domain.SessionSpec {
	plan := projectFields[ClusterPodMigrationPlan](s)
	if plan.SessionNamespace == "" {
		plan.SessionNamespace = plan.SourceNamespace
	}

	if plan.TemporaryNamespace == "" {
		plan.TemporaryNamespace = plan.SourceNamespace
	}

	return plan.Domain()
}

func ClusterReservationSpecFromDomain(spec domain.SessionSpec) ClusterReservationSpec {
	out := projectFields[ClusterReservationSpec](ClusterReservationPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	return out
}

func (s ClusterReservationSpec) Domain() domain.SessionSpec {
	plan := projectFields[ClusterReservationPlan](s)
	if plan.SessionNamespace == "" {
		plan.SessionNamespace = plan.SourceNamespace
	}

	return plan.Domain()
}

func ClusterCopySpecFromDomain(spec domain.SessionSpec) ClusterCopySpec {
	out := projectFields[ClusterCopySpec](ClusterCopyPlanFromDomain(spec))

	out.Volumes = requestedVolumes(spec.Volumes)
	if len(spec.Volumes) > 0 {
		out.DestinationStorageClass = spec.Volumes[0].StorageClass
	}

	return out
}

func (s ClusterCopySpec) Domain() domain.SessionSpec {
	plan := projectFields[ClusterCopyPlan](s)
	if plan.SessionNamespace == "" {
		plan.SessionNamespace = plan.SourceNamespace
	}

	return plan.Domain()
}

func MoveSpecFromDomain(spec domain.SessionSpec) MoveSpec {
	out := projectFields[MoveSpec](MovePlanFromDomain(spec))
	if len(spec.Volumes) > 0 {
		v := spec.Volumes[0]
		out.SourcePVC = localRefFromDomain(v.SourcePVC)
		pv := localRefFromDomain(v.SourcePV)
		out.SourcePV = &pv
		dst := plannedDestinationRefFromDomain(v.DestinationPVC)
		out.DestinationPVC = &dst
	}

	return out
}

func (s MoveSpec) Domain() domain.SessionSpec {
	plan := projectFields[MovePlan](s)
	if plan.SessionNamespace == "" {
		plan.SessionNamespace = plan.SourceNamespace
	}

	plan.Identity.SourcePVC = s.SourcePVC
	if s.SourcePV != nil {
		plan.Identity.SourcePV = *s.SourcePV
	}

	if s.DestinationPVC != nil {
		plan.Identity.DestinationPVC = *s.DestinationPVC
	} else {
		plan.Identity.DestinationPVC.Name = s.SourcePVC.Name
	}

	return plan.Domain()
}
