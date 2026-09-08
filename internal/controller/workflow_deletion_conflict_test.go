package controller

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestDeletionSpecConflictPreservesCheckpointAcrossRetries(t *testing.T) {
	for _, timing := range []struct {
		name string
		read int
	}{
		{name: "before locked read", read: 2},
		{name: "after locked read", read: 3},
	} {
		t.Run(timing.name, func(t *testing.T) {
			session := newRunnerSession("deletion-conflict")
			session.Spec.Volumes[0].SourcePV = domain.ObjectReference{
				Name: "source-pv",
				UID:  "source-pv-uid",
			}
			now := metav1.Now()
			object := &v1alpha1.Copy{
				ObjectMeta: metav1.ObjectMeta{
					Name: session.ID, Namespace: "system", UID: "workflow-uid",
					Generation: 2, DeletionTimestamp: &now,
					Finalizers: []string{kube.SessionFinalizer},
				},
				Spec:   v1alpha1.CopySpecFromDomain(session.Spec),
				Status: v1alpha1.CopyStatusFromDomain(session.Status, session.Spec.Volumes),
			}
			object.Status.Phase = v1alpha1.WorkflowPhase(domain.PhaseWarmCopied)
			plan := v1alpha1.CopyPlanFromDomain(session.Spec)
			object.Status.Plan = &plan
			object.Status.ObservedGeneration = 1

			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			base := crfake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(object).WithObjects(object).Build()
			reads := 0
			client := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, c crclient.WithWatch, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
					reads++
					if reads == timing.read {
						changed := &v1alpha1.Copy{}
						if err := c.Get(ctx, key, changed); err != nil {
							return err
						}

						changed.Generation++

						changed.Spec.DestinationStorageClass = "changed-class"
						if err := c.Update(ctx, changed); err != nil {
							return err
						}
					}

					return c.Get(ctx, key, obj, opts...)
				},
			})
			kubeClient := clientfake.NewClientset(&coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      kube.SessionLockName(session.ID),
					Namespace: "system",
					UID:       "lease-uid",
					Labels: map[string]string{
						kube.ManagedByLabel: kube.ManagedByValue,
						kube.SessionKey:     session.ID,
					},
				},
			})
			store := kube.NewCRDSessionStore(client).WithLeaseClient(kubeClient)
			service := app.NewService(kubeClient, store, nil, nil, nil, nil, app.Config{})
			r := NewWorkflowReconciler(service, store)

			request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}
			for attempt := range 3 {
				_, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy)
				if domain.CategoryOf(err) != domain.ErrorConflict {
					t.Fatalf("attempt %d: expected conflict, got %v", attempt, err)
				}

				if attempt > 0 &&
					!strings.Contains(err.Error(), "spec changed after execution started") {
					t.Fatalf("retry did not retain generation fence: %v", err)
				}
			}

			current := &v1alpha1.Copy{}
			if err := base.Get(t.Context(), request.NamespacedName, current); err != nil {
				t.Fatal(err)
			}

			if current.Generation != 3 || current.Status.ObservedGeneration != 1 ||
				current.Status.Phase != object.Status.Phase || len(current.Status.Conditions) != 0 ||
				len(current.Finalizers) != 1 || current.Finalizers[0] != kube.SessionFinalizer {
				t.Fatalf("conflict changed checkpoint or protection: %+v", current)
			}

			for _, action := range kubeClient.Actions() {
				if action.GetResource().Resource != "leases" && action.GetVerb() != "get" {
					t.Fatalf("conflict touched data-plane resources: %v", action)
				}
			}
		})
	}
}
