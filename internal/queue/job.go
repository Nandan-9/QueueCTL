// internal/queue/job.go
package queue

import "time"

type JobState string

const (
    Pending   JobState = "pending"
    Running   JobState = "running"
    Completed JobState = "completed"
    Failed    JobState = "failed"
    Dead      JobState = "dead"
)

type Job struct {
    ID        int64
    Command   string
    State     JobState
    Attempts  int
    MaxRetries int
    CreatedAt time.Time
    UpdatedAt time.Time
}
