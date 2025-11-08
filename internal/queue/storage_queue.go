package queue

import (
	"database/sql"
	"fmt"
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

	// If exceeded retries => Move to DLQ
	if j.Attempts > j.MaxRetries {
		if err := j.UpdateState(job.Dead); err != nil {
			return err
		}
		j.UpdatedAt = time.Now().UTC()
		// move to dead table & delete active
		if err := storage.MoveToDead(q.db, j); err != nil {
			return err
		}
		return nil
	}

	// else schedule retry with exponential backoff: 1s,2s,4s,8s...
	// backoff = 2^(attempts-1) seconds
	backoffSeconds := 1 << uint(j.Attempts-1)
	j.ScheduledAt = time.Now().UTC().Add(time.Duration(backoffSeconds) * time.Second)

	if err := j.UpdateState(job.Failed); err != nil {
		return err
	}
	j.UpdatedAt = time.Now().UTC()

	return storage.UpdateJob(q.db, j)
}

func (q *Queue) Retry(jobID int64) (*job.Job, error) {
	j, err := storage.GetJobByID(q.db, jobID)
	if err != nil {
		return nil, fmt.Errorf("job %d not found: %w", jobID, err)
	}

	// allow retry only failed/dead jobs
	if j.State != job.Failed  {
		return nil, fmt.Errorf("job %d is not retryable (state=%s)", jobID, j.State)
	}

	// reset state & schedule immediately
	j.State = job.Pending
	j.ScheduledAt = time.Now().UTC()


	if err := storage.UpdateJob(q.db, j); err != nil {
		return nil, err
	}

	return j, nil
}





