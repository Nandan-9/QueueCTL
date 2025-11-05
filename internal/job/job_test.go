package job

import (
	"testing"
)

func TestJobTransitions(t *testing.T) {
	j := NewJob("echo hi", 3)

	if j.State != Pending {
		t.Fatalf("expected initial state Pending, got %s", j.State)
	}

	// ✅ valid transition
	if err := j.UpdateState(Running); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ❌ invalid transition (Running -> Pending)
	if err := j.UpdateState(Pending); err == nil {
		t.Fatalf("expected error but got none")
	}
}
