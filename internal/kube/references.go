package kube

import (
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

// PodReference records the identity used to constrain workload discovery.
func PodReference(pod *corev1.Pod) domain.ObjectReference {
	if pod == nil {
		return domain.ObjectReference{}
	}

	return domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPod,
		Namespace:       pod.Namespace,
		Name:            pod.Name,
		UID:             pod.UID,
		ResourceVersion: pod.ResourceVersion,
	}
}

// PVCReference records the identity fields used to fence PVC operations.
func PVCReference(pvc *corev1.PersistentVolumeClaim) domain.ObjectReference {
	if pvc == nil {
		return domain.ObjectReference{}
	}

	return domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
}

// PVReference records the identity fields used to fence PV operations.
func PVReference(pv *corev1.PersistentVolume) domain.ObjectReference {
	if pv == nil {
		return domain.ObjectReference{}
	}

	return domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolume,
		Name:            pv.Name,
		UID:             pv.UID,
		ResourceVersion: pv.ResourceVersion,
	}
}
