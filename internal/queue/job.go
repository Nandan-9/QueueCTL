package queue

import (
    "errors"
    "time"
)

type JobState string

const (
    Pending   JobState = "pending"
    Running   JobState = "running"
    Completed JobState = "completed"
    Failed    JobState = "failed"
    Dead      JobState = "dead"
)

type Job struct {
    ID         int64
    Command    string
    State      JobState
    Attempts   int
    MaxRetries int
    CreatedAt  time.Time
    UpdatedAt  time.Time
}

// NewJob creates a new pending job with timestamps
func NewJob(command string, maxRetries int) *Job {
    now := time.Now()
    return &Job{
        Command:    command,
        State:      Pending,
        Attempts:   0,
        MaxRetries: maxRetries,
        CreatedAt:  now,
        UpdatedAt:  now,
    }
}

// UpdateState updates job state following allowed transitions
func (j *Job) UpdateState(newState JobState) error {
    if !isValidTransition(j.State, newState) {
        return errors.New("invalid state transition")
    }
    j.State = newState
    j.UpdatedAt = time.Now()
    return nil
}

// Valid transitions map
func isValidTransition(from, to JobState) bool {
    allowed := map[JobState][]JobState{
        Pending:   {Running, Dead},
        Running:   {Completed, Failed},
        Failed:    {Pending, Dead},
        Completed: {},
        Dead:      {},
    }

    for _, s := range allowed[from] {
        if s == to {
            return true
        }
    }
    return false
}
