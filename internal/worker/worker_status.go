package worker

import (
    "database/sql"
    "time"
)

// WorkerStatus represents current state of a worker
type WorkerStatus struct {
    ID           int
    State        string
    CurrentJobID int64
    UpdatedAt    time.Time
}

// UpdateWorkerStatus persists the worker status in DB
func UpdateWorkerStatus(db *sql.DB, id int, state string, jobID int64) error {
    now := time.Now().UTC()
    _, err := db.Exec(`
        INSERT INTO workers(id, state, current_job_id, updated_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            state=excluded.state,
            current_job_id=excluded.current_job_id,
            updated_at=excluded.updated_at
    `, id, state, jobID, now)
    return err
}

// GetAllWorkerStatus fetches all workers from DB
func GetAllWorkerStatus(db *sql.DB) ([]WorkerStatus, error) {
    rows, err := db.Query(`SELECT id, state, current_job_id, updated_at FROM workers ORDER BY id`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var out []WorkerStatus
    for rows.Next() {
        var w WorkerStatus
        if err := rows.Scan(&w.ID, &w.State, &w.CurrentJobID, &w.UpdatedAt); err != nil {
            return nil, err
        }
        out = append(out, w)
    }
    return out, nil
}
