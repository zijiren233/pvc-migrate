package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// ExecutionIntentHash freezes transfer inputs while allowing cleanup policies
// to change without replanning identities, volumes, or transfer settings.
func ExecutionIntentHash(intent json.RawMessage) string {
	if len(intent) == 0 {
		return ""
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(intent, &fields); err != nil {
		return ""
	}

	delete(fields, "sourcePVReclaimPolicy")
	delete(fields, "destinationPVCReclaimPolicy")

	data, err := json.Marshal(fields)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func ValidateReclaimPolicies(source, destination string) error {
	for _, field := range []struct{ name, value string }{
		{"sourcePVReclaimPolicy", source}, {"destinationPVCReclaimPolicy", destination},
	} {
		if field.value != "" && field.value != "Retain" && field.value != "Delete" {
			return NewError(
				ErrorValidation,
				"reclaim policy",
				field.name+" must be Retain or Delete",
			)
		}
	}

	return nil
}
