package v1alpha1_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestReservationTransferSettingsSurviveIntentAndPlan(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("cluster=%t/delete=%t", cluster, remove), func(t *testing.T) {
				options := domain.SessionWorkflowOptions{
					SourceNode: "source-node", TargetNode: "target-node",
					Strategies:     []string{domain.StrategyMount, domain.StrategyClusterIP},
					VerifyChecksum: true, DeleteExtraneous: remove, SkipSourceUsageCheck: true,
				}
				spec := domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
					SourceNamespace: "app", TemporaryNamespace: "archive",
					DestinationNamespace: "archive", SessionNamespace: "control",
				}, false, options)

				var intent domain.SessionSpec
				if cluster {
					payload := reservationJSONRoundTrip(
						t,
						v1alpha1.ClusterReservationSpecFromDomain(spec),
					)
					intent = payload.Domain()
				} else {
					payload := reservationJSONRoundTrip(t, v1alpha1.ReservationSpecFromDomain(spec))
					intent = payload.Domain("app")
				}

				if !reflect.DeepEqual(intent.WorkflowOptions(), options) {
					t.Fatalf(
						"intent lost transfer settings: got %+v, want %+v",
						intent.WorkflowOptions(),
						options,
					)
				}

				options.ToolImage = "registry.example/tool:v1"
				intent.WorkflowOptionsPtr().ToolImage = options.ToolImage

				var restored domain.SessionSpec
				if cluster {
					plan := reservationJSONRoundTrip(
						t,
						v1alpha1.ClusterReservationPlanFromDomain(intent),
					)
					restored = plan.Domain()
				} else {
					plan := reservationJSONRoundTrip(t, v1alpha1.ReservationPlanFromDomain(intent))
					restored = plan.Domain("app")
				}

				if !reflect.DeepEqual(restored.WorkflowOptions(), options) {
					t.Fatalf(
						"plan lost transfer settings: got %+v, want %+v",
						restored.WorkflowOptions(),
						options,
					)
				}
			})
		}
	}
}

func reservationJSONRoundTrip[T any](t *testing.T, input T) T {
	t.Helper()

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}

	var output T
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}

	return output
}
