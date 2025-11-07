package queue_test

import (
	"testing"

	"queuectl/internal/job"
	"queuectl/internal/queue"
	"queuectl/internal/storage"
)

func setupDB(t *testing.T) *queue.Queue {
	t.Helper()
	// in-memory SQLite
	db, err := storage.OpenDB(":memory:")
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	return queue.NewQueue(db)
}

func TestPushPullAck(t *testing.T) {
	q := setupDB(t)

	// Push job
	j, err := q.Push("echo hi", 3)
	if err != nil {
		t.Fatalf("push failed: %v", err)
	}

	// Pull job
	got, err := q.Pull()
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if got == nil {
		t.Fatalf("expected job, got nil")
	}

	// Ack job
	if err := q.Ack(got); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	// Check state
	loaded, err := storage.GetJobByID(q.Db(), j.ID)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if loaded.State != job.Completed {
		t.Fatalf("expected completed, got %s", loaded.State)
	}
}

func TestPullEmptyReturnsNil(t *testing.T) {
	q := setupDB(t)

	j, err := q.Pull()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if j != nil {
		t.Fatalf("expected nil job, got %+v", j)
	}
}

func TestRejectIncrementsAttemptsAndFails(t *testing.T) {
	q := setupDB(t)

	j, err := q.Push("false", 3)
	if err != nil {
		t.Fatalf("push failed: %v", err)
	}

	task, err := q.Pull()
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	if err := q.Reject(task); err != nil {
		t.Fatalf("reject failed: %v", err)
	}

	loaded, err := storage.GetJobByID(q.Db(), j.ID)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if loaded.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", loaded.Attempts)
	}
	if loaded.State != job.Failed {
		t.Fatalf("expected failed, got %s", loaded.State)
	}
}

func TestRejectMovesToDead(t *testing.T) {
	q := setupDB(t)

	j, err := q.Push("bad_cmd", 1)
	if err != nil {
		t.Fatalf("push failed: %v", err)
	}

	task, _ := q.Pull()

	// first failure → failed state
	_ = q.Reject(task)

	// second failure → dead
	_ = q.Reject(task)

	loaded, err := storage.GetJobByID(q.Db(), j.ID)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if loaded.State != job.Dead {
		t.Fatalf("expected dead, got %s", loaded.State)
	}
}
