package storage

import (
    "database/sql"
    "errors"
    "time"

    _ "github.com/mattn/go-sqlite3"

	"queuectl/internal/job"
)

// Schema for jobs table including DLQ using state column
const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);
`

// OpenDB opens SQLite and applies schema
func OpenDB(path string) (*sql.DB, error) {
    db, err := sql.Open("sqlite3", path)
    if err != nil {
        return nil, err
    }
    if err := db.Ping(); err != nil {
        return nil, err
    }
    _, err = db.Exec(schemaSQL)
    return db, err
}

// InsertJob inserts a job
func InsertJob(db *sql.DB, job *job.Job) (int64, error) {
    res, err := db.Exec(
        `INSERT INTO jobs(command, state, attempts, max_retries, created_at, updated_at)
        VALUES(?,?,?,?,?,?)`,
        job.Command, job.State, job.Attempts, job.MaxRetries, job.CreatedAt, job.UpdatedAt,
    )
    if err != nil {
        return 0, err
    }
    return res.LastInsertId()
}

// GetJobByID returns job by ID
func GetJobByID(db *sql.DB, id int64) (*job.Job, error) {
    row := db.QueryRow(
        `SELECT id, command, state, attempts, max_retries, created_at, updated_at FROM jobs WHERE id=?`, id,
    )

    var j job.Job
    if err := row.Scan(&j.ID, &j.Command, &j.State, &j.Attempts, &j.MaxRetries, &j.CreatedAt, &j.UpdatedAt); err != nil {
        return nil, err
    }
    return &j, nil
}

// PullPendingJob atomically grabs next pending job and locks it
func PullPendingJob(db *sql.DB) (*job.Job, error) {
    tx, err := db.Begin()
    if err != nil {
        return nil, err
    }

    row := tx.QueryRow(`SELECT id, command, state, attempts, max_retries, created_at, updated_at
        FROM jobs WHERE state='pending' ORDER BY id LIMIT 1`)

    var j job.Job
    if err := row.Scan(&j.ID, &j.Command, &j.State, &j.Attempts, &j.MaxRetries, &j.CreatedAt, &j.UpdatedAt); err != nil {
        tx.Rollback()
        return nil, err
    }

    // lock job by transitioning to running
    _, err = tx.Exec(`UPDATE jobs SET state='running', updated_at=? WHERE id=?`, time.Now(), j.ID)
    if err != nil {
        tx.Rollback()
        return nil, err
    }

    if err := tx.Commit(); err != nil {
        return nil, err
    }

    j.State = job.Running
    j.UpdatedAt = time.Now()
    return &j, nil
}

// UpdateJob persists state/attempt changes
func UpdateJob(db *sql.DB, j *job.Job) error {
    _, err := db.Exec(`UPDATE jobs SET state=?, attempts=?, updated_at=? WHERE id=?`,
        j.State, j.Attempts, j.UpdatedAt, j.ID,
    )
    return err
}

// Fetch jobs by state
func GetJobsByState(db *sql.DB, state job.JobState) ([]job.Job, error) {
    rows, err := db.Query(`SELECT id, command, state, attempts, max_retries, created_at, updated_at
        FROM jobs WHERE state=?`, state)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var out []job.Job
    for rows.Next() {
        var j job.Job
        if err := rows.Scan(&j.ID, &j.Command, &j.State, &j.Attempts, &j.MaxRetries, &j.CreatedAt, &j.UpdatedAt); err != nil {
            return nil, err
        }
        out = append(out, j)
    }
    return out, nil
}

// Error for no jobs available
var ErrNoJob = errors.New("no pending job")
