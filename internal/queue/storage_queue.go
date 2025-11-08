package queue

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"queuectl/internal/job"
	"queuectl/internal/storage"
)

// Queue abstracts high-level queue behaviors on top of storage.
// Queue provides high-level operations over storage.
type Queue struct {
	db *sql.DB
}

// NewQueue creates a new queue instance.
func NewQueue(db *sql.DB) *Queue {
	return &Queue{db: db}
}

// Push inserts a new pending job.
func (q *Queue) Push(command string, maxRetries int) (*job.Job, error) {
	j := job.NewJob(command, maxRetries)
	id, err := storage.InsertJob(q.db, j)
	if err != nil {
		return nil, err
	}
	j.ID = id
	return j, nil
}

// Pull atomically claims the next pending job.
func (q *Queue) Pull() (*job.Job, error) {
	j, err := storage.PullPendingJob(q.db)
	if err != nil {
		if err == storage.ErrNoJob {
			return nil, nil
		}
		return nil, err
	}
	return j, nil
}

// Ack marks job completed and deletes it from active jobs.
func (q *Queue) Ack(j *job.Job) error {
	if err := j.UpdateState(job.Completed); err != nil {
		return err
	}
	// update then delete for record-keeping; or just delete directly.
	j.UpdatedAt = time.Now().UTC()
	if err := storage.UpdateJob(q.db, j); err != nil {
		return err
	}
	return storage.DeleteJob(q.db, j.ID)
}

// Reject handles retry or moves job to DLQ when retries exhausted.
func (q *Queue) Reject(j *job.Job, lastError string) error {
    j.Attempts++
    j.LastError = lastError

    // -------------------------
    // Load runtime config
    // -------------------------

    maxRetriesStr, err := storage.ConfigGet(q.db, "max_retries")
    maxRetries := j.MaxRetries // fallback = job value

    if err == nil {
        if v, convErr := strconv.Atoi(maxRetriesStr); convErr == nil {
            maxRetries = v
        }
    }

    backoffBaseStr, err := storage.ConfigGet(q.db, "backoff_base")
    backoffBase := 2 // default exponential base=2

    if err == nil {
        if v, convErr := strconv.Atoi(backoffBaseStr); convErr == nil {
            backoffBase = v
        }
    }

    // -------------------------
    // DLQ Check
    // -------------------------

    if j.Attempts > maxRetries {
        // ensure legal transition: Running → Failed → Dead
        if j.State == job.Running {
            _ = j.UpdateState(job.Failed)
        }

        if err := j.UpdateState(job.Dead); err != nil {
            return err
        }

        j.UpdatedAt = time.Now().UTC()
        return storage.MoveToDead(q.db, j)
    }

    // -------------------------
    // Retry Backoff Logic
    // -------------------------

    // default exponential backoff logic with config base
    // delay = base^(attempts-1)
    delay := math.Pow(float64(backoffBase), float64(j.Attempts-1))
    j.ScheduledAt = time.Now().UTC().Add(time.Duration(delay) * time.Second)

    if err := j.UpdateState(job.Failed); err != nil {
        return err
    }

    j.UpdatedAt = time.Now().UTC()
    return storage.UpdateJob(q.db, j)
}








func (q *Queue) Flush(kind string) error {
	switch kind {
	case "pending":
		return storage.FlushPending(q.db)
	case "dead":
		return storage.FlushDead(q.db)
	case "all":
		return storage.FlushAll(q.db)
	default:
		return fmt.Errorf("unknown flush type: %s", kind)
	}
}
