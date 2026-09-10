package app

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReclaimPoliciesUseStableSourceAndDestinationIdentities(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseCompleted, domain.PhaseRolledBack, domain.PhaseAborted} {
		for _, source := range []string{"", "Retain", "Delete"} {
			for _, destination := range []string{"", "Retain", "Delete"} {
				t.Run(string(phase)+"/"+source+"/"+destination, func(t *testing.T) {
					session := appTestSession()
					session.Status.Phase = phase
					session.Spec.SourcePVReclaimPolicy = source
					session.Spec.DestinationPVCReclaimPolicy = destination
					session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
						Name: "new-pv",
						UID:  "new-pv-uid",
					}
					session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
					session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
						Namespace: "app",
						Name:      "data",
						UID:       "active-uid",
					}

					volumes, err := reclaimVolumes(session, CleanupOptions{})
					if err != nil {
						t.Fatal(err)
					}

					if len(volumes) != 2 || volumes[0].pv.Name != "pv-source" ||
						volumes[1].pv.Name != "new-pv" {
						t.Fatalf("identities: %+v", volumes)
					}

					if volumes[0].delete != (source == "Delete" && phase == domain.PhaseCompleted) ||
						volumes[1].delete != (destination == "Delete") {
						t.Fatalf("decisions: %+v", volumes)
					}

					if phase == domain.PhaseCompleted && volumes[1].pvc.UID != "active-uid" {
						t.Fatal("cleanup targeted obsolete staging PVC")
					}
				})
			}
		}
	}
}

func TestReclaimRetainsOldPVAndAllowsDeletingSessionWithActiveConsumer(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "destination",
		UID:  "destination-uid",
	}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       "active-uid",
	}
	client := fake.NewClientset(
		managedPV(
			"pv-source",
			"source-pv-uid",
			session.ID,
			kube.ResourceRoleRollback,
			corev1.VolumeReleased,
		),
		managedPV(
			"destination",
			"destination-uid",
			session.ID,
			kube.ResourceRoleActive,
			corev1.VolumeBound,
		),
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "data",
				UID:       "active-uid",
				Labels:    map[string]string{kube.SessionKey: session.ID},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				},
			},
		},
	)
	store := &memoryStore{}
	service := &Service{client: client, store: store}

	options := CleanupOptions{Finalize: true, DeleteSession: true}
	if err := service.validateCleanup(t.Context(), session, options); err != nil {
		t.Fatal(err)
	}

	if err := service.cleanup(t.Context(), session, options); err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(t.Context(), "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		pv.Labels[kube.SessionKey] != "" ||
		store.deletes != 1 {
		t.Fatalf("retained PV=%+v deletes=%d", pv, store.deletes)
	}
}

func TestReclaimRejectsExplicitSourceDeletionAfterRollback(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseRolledBack

	_, _, err := reclaimPolicies(session, CleanupOptions{SourcePVReclaimPolicy: "Delete"})
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("error=%v", err)
	}
}

func TestRetainedDestinationWithDeletedPVCNeverRestoresDeletePolicy(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseRolledBack
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "destination",
		UID:  "destination-uid",
	}
	session.Spec.Volumes[0].DestinationPVC.UID = "deleted-staging-pvc"
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		managedPV(
			"destination",
			"destination-uid",
			session.ID,
			kube.ResourceRoleRollback,
			corev1.VolumeReleased,
		),
	)
	service := &Service{client: client}

	volumes, err := reclaimVolumes(session, CleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.protectRetainedPVs(t.Context(), volumes); err != nil {
		t.Fatal(err)
	}

	if err := service.reclaimVolume(t.Context(), session, volumes[1], true); err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(t.Context(), "destination", metav1.GetOptions{})
	if err != nil ||
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		pv.Labels[kube.SessionKey] != "" {
		t.Fatalf("retained PV=%+v error=%v", pv, err)
	}
}
