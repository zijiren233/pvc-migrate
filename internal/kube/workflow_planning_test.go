package kube

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// Execution-stage fixtures explicitly checkpoint a controller-discovered plan.
func createPlannedTestWorkflow(
	t *testing.T,
	ctx context.Context,
	store *CRDSessionStore,
	session *domain.Session,
) {
	t.Helper()

	spec, status := session.Spec, session.Status
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	session.Spec, session.Status, session.PlanPending = spec, status, false
	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}
}
