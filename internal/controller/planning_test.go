package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func planningFixture(
	t *testing.T,
) (*WorkflowReconciler, *kube.CRDSessionStore, crclient.Client, *clientfake.Clientset, reconcile.Request) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "planning",
			Namespace:  "system",
			UID:        "workflow-uid",
			Generation: 1,
		},
		Spec: v1alpha1.CopySpec{
			Volumes: []v1alpha1.VolumeRequest{
				{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			},
		},
	}
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		Build()
	kc := clientfake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:            kube.SessionLockName("planning"),
				Namespace:       "system",
				UID:             "lease-uid",
				ResourceVersion: "1",
				Labels: map[string]string{
					kube.ManagedByLabel: kube.ManagedByValue,
					kube.SessionKey:     "planning",
				},
			},
		},
	)
	store := kube.NewCRDSessionStore(client).WithLeaseClient(kc)
	r := NewWorkflowReconciler(
		&recordingWorkflowResumer{},
		store,
	).WithKubernetesClient(kc).
		WithTrustedToolImage("example/tool:v1")

	return r, store, client, kc, reconcile.Request{
		NamespacedName: crclient.ObjectKeyFromObject(object),
	}
}

func TestPlanningCommitsSnapshotWithoutChangingIntent(t *testing.T) {
	r, store, client, _, request := planningFixture(t)
	calls := 0
	r.WithPlanner(
		func(_ context.Context, session *domain.Session, image string) (domain.SessionSpec, error) {
			calls++

			if !session.PlanPending || image != "example/tool:v1" {
				t.Fatalf("invalid planner input: %+v %q", session, image)
			}

			return resolvedPlanningSpec(session.ID), nil
		},
	)

	before := &v1alpha1.Copy{}
	if err := client.Get(t.Context(), request.NamespacedName, before); err != nil {
		t.Fatal(err)
	}

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	after := &v1alpha1.Copy{}
	if err := client.Get(t.Context(), request.NamespacedName, after); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before.Spec, after.Spec) || after.Status.Plan == nil ||
		after.Status.ObservedGeneration != 1 ||
		len(after.Finalizers) != 1 {
		t.Fatalf("bad planning commit: %+v", after)
	}

	session, err := store.GetByKind(t.Context(), "system", "planning", domain.ControllerKindCopy)
	if err != nil {
		t.Fatal(err)
	}

	if session.PlanPending || session.Spec.Volumes[0].SourcePVC.UID != "pvc-uid" {
		t.Fatalf("snapshot not decoded: %+v", session)
	}
	// A stale worker must observe the first committed plan and skip discovery.
	session.PlanPending = true
	if err := r.planWorkflow(t.Context(), session); err != nil {
		t.Fatal(err)
	}

	if calls != 1 {
		t.Fatalf("discovery ran %d times", calls)
	}
}

func TestPlanningNeverReactivatesAbortedIntent(t *testing.T) {
	r, store, client, _, request := planningFixture(t)

	session, err := store.GetByKind(t.Context(), "system", "planning", domain.ControllerKindCopy)
	if err != nil {
		t.Fatal(err)
	}

	stale := *session

	session.Status.Phase = domain.PhaseAborted
	if err := store.Update(t.Context(), session); err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Copy{}
	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	object.Generation++

	object.Spec.DestinationStorageClass = "changed"
	if err := client.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	r.WithPlanner(func(_ context.Context, _ *domain.Session, _ string) (domain.SessionSpec, error) {
		t.Fatal("aborted request was planned")
		return domain.SessionSpec{}, nil
	})

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	if err := r.planWorkflow(t.Context(), &stale); err != nil {
		t.Fatal(err)
	}
}

func TestPlanningFailureCanBeCorrectedBeforeExecution(t *testing.T) {
	r, store, client, _, request := planningFixture(t)
	calls := 0
	r.WithPlanner(func(_ context.Context, s *domain.Session, _ string) (domain.SessionSpec, error) {
		calls++
		if calls == 1 {
			return domain.SessionSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"plan",
				"source missing",
			)
		}

		return resolvedPlanningSpec(s.ID), nil
	})

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	failed, err := store.GetByKind(t.Context(), "system", "planning", domain.ControllerKindCopy)
	if err != nil {
		t.Fatal(err)
	}

	if !failed.PlanPending || failed.Status.Phase != domain.PhaseFailed ||
		failed.Status.Message != "plan: source missing" {
		t.Fatalf("failure not checkpointed: %+v", failed)
	}

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	if calls != 1 {
		t.Fatal("unchanged permanent failure was replanned")
	}

	object := &v1alpha1.Copy{}
	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	object.Spec.TargetNode = "fixed-node"

	object.Generation++
	if err := client.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	updated, err := store.GetByKind(t.Context(), "system", "planning", domain.ControllerKindCopy)
	if err != nil {
		t.Fatal(err)
	}

	if updated.PlanPending || updated.Status.Phase != domain.PhasePlanned ||
		updated.Status.ObservedGeneration != 2 {
		t.Fatalf("correction not planned: %+v", updated)
	}
}

func TestPlanningConcurrentEditAndDeletionCannotCommit(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "edit", true: "delete"}[deleting], func(t *testing.T) {
			r, _, client, kc, request := planningFixture(t)
			r.WithPlanner(
				func(ctx context.Context, s *domain.Session, _ string) (domain.SessionSpec, error) {
					object := &v1alpha1.Copy{}
					if err := client.Get(ctx, request.NamespacedName, object); err != nil {
						return domain.SessionSpec{}, err
					}

					var err error
					if deleting {
						err = client.Delete(ctx, object)
					} else {
						object.Generation++
						object.Spec.TargetNode = "new-target"
						err = client.Update(ctx, object)
					}

					if err != nil {
						return domain.SessionSpec{}, err
					}

					return resolvedPlanningSpec(s.ID), nil
				},
			)

			if _, err := r.reconcile(
				t.Context(),
				request,
				domain.ControllerKindCopy,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("expected commit conflict: %v", err)
			}

			object := &v1alpha1.Copy{}
			if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
				t.Fatal(err)
			}

			if object.Status.Plan != nil || object.Status.ObservedGeneration != 0 {
				t.Fatalf("stale plan committed: %+v", object.Status)
			}

			if deleting {
				if _, err := r.reconcile(
					t.Context(),
					request,
					domain.ControllerKindCopy,
				); err != nil {
					t.Fatal(err)
				}

				if err := client.Get(
					t.Context(),
					request.NamespacedName,
					object,
				); !apierrors.IsNotFound(
					err,
				) {
					t.Fatalf("unplanned CR not finalized: %v", err)
				}

				leases, err := kc.CoordinationV1().
					Leases("system").
					List(t.Context(), metav1.ListOptions{})
				if err != nil || len(leases.Items) != 0 {
					t.Fatalf("planning lease leaked: %+v %v", leases, err)
				}
			}
		})
	}
}

func TestPlanningLostLeaseAndFailedCommitStayPending(t *testing.T) {
	for _, failure := range []string{"lease", "commit", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			session := newRunnerSession("pending")
			session.PlanPending = true
			session.Intent = []byte(`{"volumes":[{"sourcePVC":{"name":"data"}}]}`)
			lock := &runnerSessionLock{}
			store := &runnerSessionStore{latest: cloneRunnerSession(session), lock: lock}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			r := NewWorkflowReconciler(
				nil,
				store,
			).WithKubernetesClient(clientfake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}}))
			r.WithPlanner(
				func(context.Context, *domain.Session, string) (domain.SessionSpec, error) {
					switch failure {
					case "lease":
						lock.err = errors.New("lease lost")
					case "commit":
						store.updateErr = errors.New("commit failed")
					case "cancel":
						cancel()
						return domain.SessionSpec{}, ctx.Err()
					}

					return resolvedPlanningSpec("pending"), nil
				},
			)

			if err := r.planWorkflow(ctx, session); err == nil {
				t.Fatal("expected planning failure")
			}

			if !session.PlanPending || len(store.updates) != 0 || !lock.released {
				t.Fatalf("uncommitted plan exposed: %+v", session)
			}
		})
	}
}

func resolvedPlanningSpec(id string) domain.SessionSpec {
	spec := newRunnerSession(id).Spec
	spec.Volumes[0].SourcePV = domain.ObjectReference{Name: "source-pv", UID: "pv-uid"}
	return spec
}
