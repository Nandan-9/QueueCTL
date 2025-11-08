package storage

import (
	"database/sql"
	"errors"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"queuectl/internal/job"
)

// Schema includes scheduled_at and a separate dead_jobs table.
const schemaSQL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    command TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    scheduled_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS dead_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    orig_id INTEGER,
    command TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    created_at DATETIME NOT NULL,
    failed_at DATETIME NOT NULL,
    last_error TEXT
);
`

// ErrNoJob returned when no pending job available
var ErrNoJob = errors.New("no pending job")

// OpenDB opens SQLite and applies schema
func OpenDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// InsertJob inserts a job and returns id.
func InsertJob(db *sql.DB, j *job.Job) (int64, error) {
	res, err := db.Exec(
		`INSERT INTO jobs(command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error)
         VALUES(?,?,?,?,?,?,?,?)`,
		j.Command, string(j.State), j.Attempts, j.MaxRetries, j.ScheduledAt, j.CreatedAt, j.UpdatedAt, j.LastError,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// GetJobByID returns job by ID
func GetJobByID(db *sql.DB, id int64) (*job.Job, error) {
	row := db.QueryRow(
		`SELECT id, command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error
         FROM jobs WHERE id=?`, id,
	)
	var j job.Job
	var state string
	if err := row.Scan(&j.ID, &j.Command, &state, &j.Attempts, &j.MaxRetries, &j.ScheduledAt, &j.CreatedAt, &j.UpdatedAt, &j.LastError); err != nil {
		return nil, err
	}
	j.State = job.JobState(state)
	return &j, nil
}

// PullPendingJob atomically grabs next pending job that is scheduled to run.
func PullPendingJob(db *sql.DB) (*job.Job, error) {
	// Start a transaction to claim a job.
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		// if not committed, rollback in case of panic/early return
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()

	// Select one job ready to run.
	row := tx.QueryRow(`SELECT id, command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error
        FROM jobs
        WHERE state = ? AND scheduled_at <= ?
        ORDER BY id
        LIMIT 1`, string(job.Pending), now)

	var j job.Job
	var state string
	if err := row.Scan(&j.ID, &j.Command, &state, &j.Attempts, &j.MaxRetries, &j.ScheduledAt, &j.CreatedAt, &j.UpdatedAt, &j.LastError); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNoJob
		}
		return nil, err
	}
	j.State = job.JobState(state)

	// Mark as running and update timestamp.
	_, err = tx.Exec(`UPDATE jobs SET state=?, updated_at=? WHERE id=?`, string(job.Running), now, j.ID)
	if err != nil {
		return nil, err
	}

	// Commit claim.
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	j.State = job.Running
	j.UpdatedAt = now
	return &j, nil
}

// UpdateJob updates state/attempts/scheduled_at/last_error
func UpdateJob(db *sql.DB, j *job.Job) error {
	_, err := db.Exec(`UPDATE jobs SET state=?, attempts=?, scheduled_at=?, updated_at=?, last_error=? WHERE id=?`,
		string(j.State), j.Attempts, j.ScheduledAt, j.UpdatedAt, j.LastError, j.ID,
	)
	return err
}

// DeleteJob removes a job from active jobs table.
func DeleteJob(db *sql.DB, id int64) error {
	_, err := db.Exec(`DELETE FROM jobs WHERE id = ?`, id)
	return err
}

// MoveToDead moves a job to dead_jobs table (for manual inspection) and removes from active jobs.
func MoveToDead(db *sql.DB, j *job.Job) error {
	now := time.Now().UTC()
	_, err := db.Exec(`INSERT INTO dead_jobs(orig_id, command, attempts, max_retries, created_at, failed_at, last_error)
        VALUES(?,?,?,?,?,?,?)`,
		j.ID, j.Command, j.Attempts, j.MaxRetries, j.CreatedAt, now, j.LastError,
	)
	if err != nil {
		return err
	}
	// remove from active jobs
	return DeleteJob(db, j.ID)
}

// GetJobsByState fetches jobs from active table by state.
func GetJobsByState(db *sql.DB, state job.JobState) ([]job.Job, error) {
	rows, err := db.Query(`SELECT id, command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error
        FROM jobs WHERE state=? ORDER BY id`, string(state))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []job.Job
	for rows.Next() {
		var j job.Job
		var st string
		if err := rows.Scan(&j.ID, &j.Command, &st, &j.Attempts, &j.MaxRetries, &j.ScheduledAt, &j.CreatedAt, &j.UpdatedAt, &j.LastError); err != nil {
			return nil, err
		}
		j.State = job.JobState(st)
		out = append(out, j)
	}
	return out, nil
}

// ListDeadJobs returns dead_jobs for inspection
type DeadJob struct {
	ID        int64
	OrigID    sql.NullInt64
	Command   string
	Attempts  int
	MaxRetries int
	CreatedAt time.Time
	FailedAt  time.Time
	LastError sql.NullString
}

func ListDeadJobs(db *sql.DB) ([]DeadJob, error) {
	rows, err := db.Query(`SELECT id, orig_id, command, attempts, max_retries, created_at, failed_at, last_error FROM dead_jobs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeadJob
	for rows.Next() {
		var d DeadJob
		if err := rows.Scan(&d.ID, &d.OrigID, &d.Command, &d.Attempts, &d.MaxRetries, &d.CreatedAt, &d.FailedAt, &d.LastError); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}



