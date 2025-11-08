package storage

import (
	"database/sql"
	"queuectl/internal/job"
	
)



func CountJobsByState(db *sql.DB, state job.JobState) (int, error) {
    var count int
    row := db.QueryRow("SELECT COUNT(*) FROM jobs WHERE state = ?", string(state))
    if err := row.Scan(&count); err != nil {
        return 0, err
    }
    return count, nil
}


func GetNextScheduledJob(db *sql.DB) (*job.Job, error) {
    row := db.QueryRow(`
        SELECT id, command, attempts, max_retries, state, scheduled_at, created_at, updated_at, last_error
        FROM jobs
        WHERE state = 'pending'
        ORDER BY scheduled_at ASC
        LIMIT 1
    `)

    var j job.Job
    err := row.Scan(
        &j.ID, &j.Command, &j.Attempts, &j.MaxRetries, &j.State, &j.ScheduledAt,
        &j.CreatedAt, &j.UpdatedAt, &j.LastError,
    )
    if err != nil {
        return nil, err
    }
    return &j, nil
}
