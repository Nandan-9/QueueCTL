package queue

import (
	"database/sql"
	"time"

	"queuectl/internal/job"
	"queuectl/internal/storage"
)

// Queue abstracts high-level queue behaviors on top of storage.
type Queue struct {
	db *sql.DB
}

// NewQueue creates a new queue instance.
func NewQueue(db *sql.DB) *Queue {
	return &Queue{db: db}
}

func (q *Queue) Db() *sql.DB {
    return q.db
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
		// no pending job case
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return j, nil
}

// Ack finalizes a job as completed.
func (q *Queue) Ack(j *job.Job) error {
	if err := j.UpdateState(job.Completed); err != nil {
		return err
	}
	return storage.UpdateJob(q.db, j)
}

// Reject handles retry or marks dead.
func (q *Queue) Reject(j *job.Job) error {
	j.Attempts++

	if j.Attempts > j.MaxRetries {
		if err := j.UpdateState(job.Dead); err != nil {
			return err
		}
	} else {
		if err := j.UpdateState(job.Failed); err != nil {
			return err
		}
	}

	j.UpdatedAt = time.Now()
	return storage.UpdateJob(q.db, j)
}
