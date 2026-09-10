package controller

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// The caller owns fixture creation and cleanup and holds its real Lease to
// exclude deployed controllers while this reconciler injects a precise race.
func TestDeletionSpecConflictLive(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_DELETION_RACE_TEST") != "1" {
		t.Skip("requires a deleting Copy fixture with an externally held test Lease")
	}

	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	base, err := crclient.New(config, crclient.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	key := crclient.ObjectKey{
		Namespace: os.Getenv("PVC_MIGRATE_TEST_NAMESPACE"),
		Name:      os.Getenv("PVC_MIGRATE_TEST_WORKFLOW"),
	}

	owner := os.Getenv("PVC_MIGRATE_TEST_LEASE_HOLDER")
	if key.Namespace == "" || key.Name == "" || owner == "" {
		t.Fatal("fixture identity and Lease holder are required")
	}

	verifyDeletionTestLease(t, kubeClient, key, owner)

	original := &v1alpha1.Copy{}
	if err := base.Get(t.Context(), key, original); err != nil {
		t.Fatal(err)
	}

	if original.DeletionTimestamp == nil || original.Status.Phase != "WarmCopied" ||
		original.Status.Plan == nil || len(original.Status.Plan.Volumes) != 1 ||
		original.Spec.DestinationStorageClass == "changed-class" {
		t.Fatal("requires deleting completed Copy with retained source")
	}

	client := &deletionRaceClient{Client: base, injectRead: deletionTestRead(t)}
	store := &externallyLockedDeletionStore{
		ControllerSessionStore: kube.NewCRDSessionStore(client).WithLeaseClient(kubeClient),
	}
	service := app.NewService(kubeClient, store, nil, nil, nil, nil, app.Config{})

	r := NewWorkflowReconciler(service, store)
	for attempt := range 1 {
		_, err := r.reconcile(
			t.Context(),
			reconcile.Request{NamespacedName: key},
			domain.ControllerKindCopy,
		)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("attempt %d: %v", attempt, err)
		}

		t.Logf("attempt=%d conflict=%v", attempt, err)
	}

	current := &v1alpha1.Copy{}
	if err := base.Get(t.Context(), key, current); err != nil {
		t.Fatal(err)
	}

	if current.UID != original.UID || current.Generation != original.Generation+1 ||
		current.Status.ObservedGeneration != original.Status.ObservedGeneration ||
		current.Status.Phase != original.Status.Phase ||
		len(current.Finalizers) == 0 {
		t.Fatal("checkpoint or protection changed")
	}

	pv, err := kubeClient.CoreV1().
		PersistentVolumes().
		Get(t.Context(), original.Status.Plan.Volumes[0].SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.UID != original.Status.Plan.Volumes[0].SourcePV.UID ||
		pv.Spec.PersistentVolumeReclaimPolicy != "Retain" ||
		pv.Labels[kube.SessionKey] != key.Name {
		t.Fatal("source storage changed")
	}
}

func deletionTestRead(t *testing.T) int {
	t.Helper()

	read, err := strconv.Atoi(os.Getenv("PVC_MIGRATE_TEST_INJECT_READ"))
	if err != nil || (read != 2 && read != 3) {
		t.Fatal("inject read must be 2 or 3")
	}

	return read
}

func verifyDeletionTestLease(
	t *testing.T,
	client kubernetes.Interface,
	key crclient.ObjectKey,
	owner string,
) {
	t.Helper()

	lease, err := client.CoordinationV1().Leases(key.Namespace).
		Get(t.Context(), kube.SessionLockName(key.Name), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != owner ||
		lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil ||
		time.Until(
			lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second),
		) < time.Minute {
		t.Fatal("fixture Lease must be held for at least another minute")
	}
}

type deletionRaceClient struct {
	crclient.Client
	reads      int
	injectRead int
}

func (c *deletionRaceClient) Get(
	ctx context.Context,
	key crclient.ObjectKey,
	object crclient.Object,
	opts ...crclient.GetOption,
) error {
	c.reads++
	if c.reads == c.injectRead {
		changed := &v1alpha1.Copy{}
		if err := c.Client.Get(ctx, key, changed); err != nil {
			return err
		}

		changed.Spec.DestinationStorageClass = "changed-class"
		if err := c.Update(ctx, changed); err != nil {
			return err
		}
	}

	return c.Client.Get(ctx, key, object, opts...)
}

type externallyLockedDeletionStore struct{ kube.ControllerSessionStore }

func (*externallyLockedDeletionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return &runnerSessionLock{}, nil
}
