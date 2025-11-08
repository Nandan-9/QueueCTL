package job

import (
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
    ScheduledAt time.Time
    LastError string

}

// NewJob creates a new pending job with timestamps
func NewJob(command string, maxRetries int) *Job {
    now := time.Now().UTC()
    return &Job{
        Command:    command,
        State:      Pending,
        Attempts:   0,
        MaxRetries: maxRetries,
        CreatedAt:  now,
        UpdatedAt:  now,
        ScheduledAt: now,
    }
}

// UpdateState updates job state following allowed transitions
func (j *Job) UpdateState(newState JobState) error {
	if !isValidTransition(j.State, newState) {
		return &ErrInvalidTransition{From: j.State, To: newState}
	}
	j.State = newState
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// invalid transition error type
type ErrInvalidTransition struct {
	From JobState
	To   JobState
}

func (e *ErrInvalidTransition) Error() string {
	return "invalid state transition"
}

// Valid transitions map
func isValidTransition(from, to JobState) bool {
    allowed := map[JobState][]JobState{
        Pending:   {Running, Dead},
        Running:   {Completed, Failed, Dead}, // ✅ add Dead here
        Failed:    {Pending, Dead, Failed},   // ✅ allow retry cycles
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
