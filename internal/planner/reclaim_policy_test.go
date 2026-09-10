package planner

import (
	"encoding/json"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestControllerSubmissionPreservesApplicableReclaimPolicies(t *testing.T) {
	for _, op := range []domain.Operation{domain.OperationCopy, domain.OperationReserve, domain.OperationMigrate, domain.OperationMigratePod} {
		for _, cluster := range []bool{false, true} {
			t.Run(
				string(op)+"/cluster="+map[bool]string{false: "false", true: "true"}[cluster],
				func(t *testing.T) {
					options := planOptions{
						Operation:                   op,
						SessionID:                   "policy",
						SourceNamespace:             "app",
						DestinationNamespace:        "app",
						TemporaryNamespace:          "app",
						SessionNamespace:            "app",
						SourcePVCs:                  []string{"data"},
						SourcePVReclaimPolicy:       "Delete",
						DestinationPVCReclaimPolicy: "Delete",
					}
					if cluster {
						options.TemporaryNamespace = "temporary"
					}

					if op == domain.OperationMigratePod {
						options.PodName = "database-0"
					}

					plan, err := (&Planner{}).intentPlan(options)
					if err != nil {
						t.Fatal(err)
					}

					var intent map[string]any
					if err := json.Unmarshal(plan.Intent, &intent); err != nil {
						t.Fatal(err)
					}

					if intent["destinationPVCReclaimPolicy"] != "Delete" ||
						plan.SessionSpec.DestinationPVCReclaimPolicy != "Delete" {
						t.Fatalf("lost destination policy: %s", plan.Intent)
					}

					if op == domain.OperationCopy || op == domain.OperationReserve {
						if _, ok := intent["sourcePVReclaimPolicy"]; ok ||
							plan.SessionSpec.SourcePVReclaimPolicy != "" {
							t.Fatalf("copy/reservation exposed source policy: %s", plan.Intent)
						}
					} else if intent["sourcePVReclaimPolicy"] != "Delete" || plan.SessionSpec.SourcePVReclaimPolicy != "Delete" {
						t.Fatalf("lost source policy: %s", plan.Intent)
					}
				},
			)
		}
	}
}
