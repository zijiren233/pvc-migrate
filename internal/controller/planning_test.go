package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
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
	clienttesting "k8s.io/client-go/testing"
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
		WithStatusSubresource(object, &v1alpha1.Backup{}, &v1alpha1.Restore{}).
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

func TestRepositoryPlanningFailureAllowsCorrectedIntent(t *testing.T) {
	for _, kind := range []domain.ControllerKind{domain.ControllerKindBackup, domain.ControllerKindRestore} {
		for _, failure := range []string{"missing repository", "missing credentials", "unsupported backend"} {
			t.Run(string(kind)+"/"+failure, func(t *testing.T) {
				r, store, client, kc, request := planningFixture(t)
				r.WithControllerClient(client)

				if err := client.Delete(
					t.Context(),
					&v1alpha1.Copy{
						ObjectMeta: metav1.ObjectMeta{Name: "planning", Namespace: "system"},
					},
				); err != nil {
					t.Fatal(err)
				}

				metadata := metav1.ObjectMeta{
					Name:       "planning",
					Namespace:  "system",
					UID:        "workflow-uid",
					Generation: 1,
				}

				var object crclient.Object = &v1alpha1.Backup{ObjectMeta: metadata, Spec: v1alpha1.BackupSpec{
					SourcePVC: v1alpha1.LocalResourceReference{
						Name: "data",
					},
					Name:          "point",
					RepositoryRef: v1alpha1.LocalObjectReference{Name: "broken"},
				}}
				if kind == domain.ControllerKindRestore {
					object = &v1alpha1.Restore{ObjectMeta: metadata, Spec: v1alpha1.RestoreSpec{
						DestinationPVC: v1alpha1.LocalResourceReference{
							Name: "data",
						},
						Name:          "point",
						RepositoryRef: v1alpha1.LocalObjectReference{Name: "broken"},
					}}
				}

				if err := client.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}

				repository := &v1alpha1.BackupRepository{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "broken",
						Namespace:  "system",
						UID:        "repository-uid",
						Generation: 3,
					},
					Spec: v1alpha1.BackupRepositorySpec{
						Type: v1alpha1.BackupRepositoryTypeS3,
						S3: &v1alpha1.S3BackupRepositorySpec{
							Bucket:   "backups",
							Endpoint: "https://s3.example.test",
							CredentialsSecret: v1alpha1.BackupRepositorySecretReference{
								Name: "credentials",
							},
						},
					},
				}
				if failure == "unsupported backend" {
					repository.Spec.Type = v1alpha1.BackupRepositoryTypePVC
				}

				if failure != "missing repository" {
					if err := client.Create(t.Context(), repository); err != nil {
						t.Fatal(err)
					}
				}

				calls := 0
				r.WithPlanner(
					func(_ context.Context, session *domain.Session, _ string) (domain.SessionSpec, error) {
						calls++

						if session.Spec.Backup != nil {
							session.Spec.Backup.SourcePVC.UID = "source-uid"
							session.Spec.Backup.SourcePV = domain.ObjectReference{
								Name: "source-pv",
								UID:  "source-pv-uid",
							}
						}

						return session.Spec, nil
					},
				)

				if _, err := r.reconcile(t.Context(), request, kind); err != nil {
					t.Fatal(err)
				}

				failed, err := store.GetByKind(t.Context(), "system", "planning", kind)
				if err != nil {
					t.Fatal(err)
				}

				if !failed.PlanPending || failed.Status.Phase != domain.PhaseFailed || calls != 0 ||
					!strings.Contains(failed.Status.Message, "BackupRepository") {
					t.Fatalf(
						"invalid repository froze the intent: pending=%v phase=%s calls=%d message=%s",
						failed.PlanPending,
						failed.Status.Phase,
						calls,
						failed.Status.Message,
					)
				}

				repository.Name, repository.ResourceVersion = "corrected", ""

				repository.Spec.Type = v1alpha1.BackupRepositoryTypeS3
				if err := client.Create(t.Context(), repository); err != nil {
					t.Fatal(err)
				}

				if _, err := kc.CoreV1().Secrets("system").Create(t.Context(), &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "credentials",
						Namespace: "system",
						UID:       "credentials-uid",
					},
					Data: map[string][]byte{
						kube.BackupAccessKeyDataKey: []byte("access"),
						kube.BackupSecretKeyDataKey: []byte("secret"),
					},
				}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}

				if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
					t.Fatal(err)
				}

				switch workflow := object.(type) {
				case *v1alpha1.Backup:
					workflow.Spec.RepositoryRef.Name = repository.Name
				case *v1alpha1.Restore:
					workflow.Spec.RepositoryRef.Name = repository.Name
				}

				object.SetGeneration(2)

				if err := client.Update(t.Context(), object); err != nil {
					t.Fatal(err)
				}

				if _, err := r.reconcile(t.Context(), request, kind); err != nil {
					t.Fatal(err)
				}

				planned, err := store.GetByKind(t.Context(), "system", "planning", kind)
				if err != nil {
					t.Fatal(err)
				}

				binding := planned.Status.BackupRepository
				if planned.PlanPending || calls != 1 || binding == nil ||
					binding.UID != repository.UID ||
					binding.Generation != repository.Generation ||
					binding.S3.CredentialsSecretUID != "credentials-uid" {
					t.Fatalf(
						"corrected repository was not planned and pinned: pending=%v calls=%d binding=%+v",
						planned.PlanPending,
						calls,
						binding,
					)
				}
			})
		}
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

func TestUnplannedDeletionRetainsFinalizerUntilLeaseCleanup(t *testing.T) {
	r, store, client, kc, request := planningFixture(t)

	session, err := store.GetByKind(t.Context(), "system", "planning", domain.ControllerKindCopy)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.EnsureSessionProtection(t.Context(), session); err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Copy{}
	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	blocked := true
	kc.PrependReactor("delete", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
		if blocked {
			return true, nil, errors.New("Lease API unavailable")
		}
		return false, nil, nil
	})

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err == nil {
		t.Fatal("expected Lease cleanup failure")
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatalf("lost cleanup retry anchor: %v", err)
	}

	if len(object.Finalizers) == 0 {
		t.Fatal("removed finalizer before Lease cleanup")
	}

	blocked = false

	if _, err := r.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup retry did not delete workflow: %v", err)
	}
}
