package controller

import (
	"encoding/json"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestOnlyReclaimPoliciesCanChangeAfterExecutionStarts(t *testing.T) {
	original := json.RawMessage(
		`{"volumes":[{"sourcePVC":{"name":"data"}}],"sourcePVReclaimPolicy":"Retain"}`,
	)
	for _, tc := range []struct {
		intent  string
		allowed bool
	}{
		{`{"volumes":[{"sourcePVC":{"name":"data"}}],"sourcePVReclaimPolicy":"Delete","destinationPVCReclaimPolicy":"Retain"}`, true},
		{`{"volumes":[{"sourcePVC":{"name":"other"}}],"sourcePVReclaimPolicy":"Delete"}`, false},
	} {
		session := &domain.Session{
			Generation: 3,
			Intent:     json.RawMessage(tc.intent),
			Status: domain.SessionStatus{
				ObservedGeneration:  1,
				ExecutionIntentHash: domain.ExecutionIntentHash(original),
			},
		}
		for _, generation := range []int64{1, 2, 3} {
			session.Generation = generation
			for _, deleting := range []bool{false, true} {
				session.Deleting = deleting
				if err := workflowSpecMutationError(session); (err == nil) != tc.allowed {
					t.Fatalf(
						"generation=%d deleting=%v intent=%s err=%v",
						generation,
						deleting,
						tc.intent,
						err,
					)
				}
			}
		}
	}
}
