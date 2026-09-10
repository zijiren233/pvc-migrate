package planner

import (
	"encoding/json"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestWorkflowPlansMinimalVolumeIntent(t *testing.T) {
	for _, kind := range []domain.ControllerKind{domain.ControllerKindCopy, domain.ControllerKindMigration, domain.ControllerKindReservation} {
		t.Run(string(kind), func(t *testing.T) {
			request, err := intentPlanForKind(
				"minimal",
				"app",
				kind,
				map[string]any{
					"volumes": []map[string]any{{"sourcePVC": map[string]string{"name": "data"}}},
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			client := plannerClient(plannerObjects("2Gi")...)
			p := New(client, nil)
			session := domain.NewSession(request.SessionID, request.SessionSpec, metav1.Now().Time)
			session.Intent = request.Intent

			spec, err := p.PlanWorkflow(t.Context(), session, "example/tool:v1")
			if err != nil {
				t.Fatal(err)
			}

			if len(spec.Volumes) != 1 || spec.Volumes[0].SourcePVC.UID != "pvc-uid" ||
				spec.Volumes[0].SourcePV.UID != "pv-uid" ||
				spec.Volumes[0].Capacity != "2Gi" ||
				spec.WorkflowOptions().ToolImage != "example/tool:v1" {
				t.Fatalf("incomplete resolved plan: %+v", spec)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("planning mutated cluster: %v", action)
				}
			}
		})
	}
}

func TestWorkflowHonorsIdentityConstraints(t *testing.T) {
	for _, test := range []struct {
		name      string
		pvc, pv   v1alpha1.LocalResourceReference
		wantError bool
	}{
		{name: "matching", pvc: v1alpha1.LocalResourceReference{Name: "data", UID: "pvc-uid"}, pv: v1alpha1.LocalResourceReference{Name: "pv-source", UID: "pv-uid"}},
		{name: "wrong pvc uid", pvc: v1alpha1.LocalResourceReference{Name: "data", UID: "replaced"}, wantError: true},
		{name: "wrong pv", pvc: v1alpha1.LocalResourceReference{Name: "data"}, pv: v1alpha1.LocalResourceReference{Name: "different"}, wantError: true},
		{name: "stale resource version", pvc: v1alpha1.LocalResourceReference{Name: "data", ResourceVersion: "1"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			volume := v1alpha1.VolumeRequest{SourcePVC: test.pvc}
			if test.pv.Name != "" {
				volume.SourcePV = &test.pv
			}

			request, err := intentPlanForKind(
				"identity",
				"app",
				domain.ControllerKindCopy,
				v1alpha1.CopySpec{Volumes: []v1alpha1.VolumeRequest{volume}},
			)
			if err != nil {
				t.Fatal(err)
			}

			session := domain.NewSession(request.SessionID, request.SessionSpec, metav1.Now().Time)
			session.Intent = request.Intent

			_, err = New(
				plannerClient(plannerObjects("2Gi")...),
				nil,
			).PlanWorkflow(t.Context(), session, "example/tool:v1")
			if test.wantError {
				if domain.CategoryOf(err) != domain.ErrorConflict {
					t.Fatalf("expected identity conflict, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkflowPodSelectionAllowsPartialOverrides(t *testing.T) {
	objects := plannerObjectsWithTwoPVCs(t)
	objects = append(
		objects,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "app", UID: "pod-uid"},
			Spec: corev1.PodSpec{NodeName: "node-b", Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
				{
					Name: "logs",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "logs",
						},
					},
				},
			}},
		},
	)

	request, err := intentPlanForKind(
		"partial",
		"app",
		domain.ControllerKindCopy,
		v1alpha1.CopySpec{
			Pod:    &v1alpha1.LocalResourceReference{Name: "writer", UID: "pod-uid"},
			Online: true,
			Volumes: []v1alpha1.VolumeRequest{
				{
					SourcePVC:      v1alpha1.LocalResourceReference{Name: "logs"},
					Capacity:       "3Gi",
					DestinationPVC: &v1alpha1.LocalResourceReference{Name: "logs-copy"},
					TransferScope: &v1alpha1.TransferScope{
						SourcePath:      "logs",
						DestinationPath: "saved",
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	session := domain.NewSession(request.SessionID, request.SessionSpec, metav1.Now().Time)
	session.Intent = request.Intent

	spec, err := New(
		plannerClient(objects...),
		nil,
	).PlanWorkflow(t.Context(), session, "example/tool:v1")
	if err != nil {
		t.Fatal(err)
	}

	if len(spec.Volumes) != 2 {
		t.Fatalf("selected %d volumes", len(spec.Volumes))
	}

	for _, v := range spec.Volumes {
		if v.SourcePVC.Name == "logs" {
			if v.Capacity != "3Gi" || v.DestinationPVC.Name != "logs-copy" ||
				v.TransferScope.SourcePath != "logs" {
				t.Fatalf("override lost: %+v", v)
			}
		} else if v.Capacity != "2Gi" {
			t.Fatalf("default capacity lost: %+v", v)
		}
	}
}

func TestControllerSubmissionDoesNotReadSource(t *testing.T) {
	client := plannerClient()

	request, err := New(
		client,
		nil,
	).ForSubmission(true).
		PlanCopy(t.Context(), CopyOptions{SessionID: "request", SourceNamespace: "app", SourcePVCs: []string{"missing"}})
	if err != nil {
		t.Fatal(err)
	}

	if len(client.Actions()) != 0 {
		t.Fatalf("submission discovered resources: %v", client.Actions())
	}

	if len(request.Intent) == 0 || strings.Contains(string(request.Intent), `"sourcePV":`) {
		t.Fatalf("unexpected request: %s", request.Intent)
	}

	object := &v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: "app"}}
	if err := json.Unmarshal(request.Intent, &object.Spec); err != nil {
		t.Fatal(err)
	}

	session, err := kube.DecodeWorkflow(object)
	if err != nil || !session.PlanPending {
		t.Fatalf("unplanned workflow: %+v %v", session, err)
	}
}

func TestReservationSubmissionPreservesFutureCopyScopeAndSettings(t *testing.T) {
	for _, destination := range []string{"app", "system"} {
		t.Run(destination, func(t *testing.T) {
			request, err := New(
				plannerClient(),
				nil,
			).ForSubmission(true).
				PlanReserve(t.Context(), ReserveOptions{
					SessionID:            "reserved",
					SourceNamespace:      "app",
					DestinationNamespace: destination,
					TemporaryNamespace:   destination,
					SessionNamespace:     "app",
					SourcePVCs: []string{
						"data",
					},
					SourcePaths:      []string{"sub"},
					DestinationPaths: []string{"archive"},
					SourceNode:       "node-a",
					Strategies:       []string{domain.StrategyMount},
					VerifyChecksum:   true,
					DeleteExtraneous: true,
				})
			if err != nil {
				t.Fatal(err)
			}

			var body map[string]any
			if err := json.Unmarshal(request.Intent, &body); err != nil {
				t.Fatal(err)
			}

			if body["sourcePath"] != "sub" || body["destinationPath"] != "archive" ||
				body["sourceNode"] != "node-a" ||
				body["verifyChecksum"] != true ||
				body["deleteExtraneous"] != true {
				t.Fatalf("reservation lost future copy intent: %s", request.Intent)
			}

			if got, ok := body["strategies"].([]any); !ok || len(got) != 1 ||
				got[0] != domain.StrategyMount {
				t.Fatalf("reservation lost copy strategy: %s", request.Intent)
			}
		})
	}
}

func TestControllerSubmissionRejectsAmbiguousMappings(t *testing.T) {
	for _, capacities := range [][]string{{"1Gi", "2Gi"}, {"1Gi", "data=2Gi"}, {"data=1Gi", "data=2Gi"}, {"data="}} {
		_, err := New(plannerClient(), nil).ForSubmission(true).PlanCopy(t.Context(), CopyOptions{
			SessionID:             "request",
			SourceNamespace:       "app",
			SourcePVCs:            []string{"data"},
			DestinationCapacities: capacities,
		})
		if err == nil {
			t.Fatalf("accepted ambiguous mapping %v", capacities)
		}
	}
}

func TestControllerSubmissionSinglePathDefaultsToVolumeRoot(t *testing.T) {
	request, err := New(plannerClient(), nil).ForSubmission(true).PlanCopy(t.Context(), CopyOptions{
		SessionID:          "request",
		SourceNamespace:    "app",
		SourcePVCs:         []string{"data"},
		SourcePaths:        []string{"data=logs"},
		SessionNamespace:   "app",
		TemporaryNamespace: "app",
	})
	if err != nil {
		t.Fatal(err)
	}

	session := domain.NewSession(request.SessionID, request.SessionSpec, metav1.Now().Time)
	session.Intent = request.Intent

	spec, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanWorkflow(t.Context(), session, "example/tool:v1")
	if err != nil {
		t.Fatal(err)
	}

	if scope := spec.Volumes[0].TransferScope; scope == nil || scope.SourcePath != "logs" ||
		scope.DestinationPath != domain.VolumeRootPath {
		t.Fatalf("unexpected scope: %+v", scope)
	}
}

func TestRestorePlanningPinsExistingDestinationAndHonorsConstraints(t *testing.T) {
	for _, test := range []struct {
		name             string
		existing, create bool
		uid              string
		fail             bool
	}{
		{"existing", true, false, "", false},
		{"existing create", true, true, "pvc-uid", false},
		{"wrong uid", true, true, "replaced", true},
		{"create missing", false, true, "", false},
		{"pinned missing", false, true, "pvc-uid", true},
		{"missing", false, false, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := intentPlanForKind(
				"restore",
				"app",
				domain.ControllerKindRestore,
				v1alpha1.RestoreSpec{
					DestinationPVC: v1alpha1.LocalResourceReference{
						Name: "data",
						UID:  types.UID(test.uid),
					},
					RepositoryRef: v1alpha1.LocalObjectReference{
						Name: "repository",
					},
					Name:      "archive",
					CreatePVC: test.create,
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			session := domain.NewSession(request.SessionID, request.SessionSpec, metav1.Now().Time)
			session.Intent = request.Intent

			client := plannerClient()
			if test.existing {
				client = plannerClient(plannerObjects("2Gi")...)
			}

			spec, err := New(client, nil).PlanWorkflow(t.Context(), session, "example/tool:v1")
			if (err != nil) != test.fail {
				t.Fatalf("error=%v want failure=%v", err, test.fail)
			}

			if err == nil && test.existing && spec.Restore.DestinationPVC.UID != "pvc-uid" {
				t.Fatal("resolved UID not pinned")
			}

			if err == nil && test.create &&
				spec.Restore.DestinationAccessMode != string(corev1.ReadWriteOnce) {
				t.Fatal("restore PVC creation did not resolve the default access mode")
			}
		})
	}
}
