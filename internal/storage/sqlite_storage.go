package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"queuectl/internal/job"
)

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

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO config (key, value) VALUES ('max_retries', '3');
INSERT OR IGNORE INTO config (key, value) VALUES ('backoff_base', '2');
INSERT OR IGNORE INTO config (key, value) VALUES ('log_level', 'info');
INSERT OR IGNORE INTO config (key, value) VALUES ('worker_concurrency', '1');
`

var ErrNoJob = errors.New("no pending job")

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

func InsertJob(db *sql.DB, j *job.Job) (int64, error) {
	res, err := db.Exec(
		`INSERT INTO jobs(command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error)
         VALUES(?,?,?,?,?,?,?,?)`,
		j.Command, string(j.State), j.Attempts, j.MaxRetries,
		j.ScheduledAt.Format(time.RFC3339),
		j.CreatedAt.Format(time.RFC3339),
		j.UpdatedAt.Format(time.RFC3339),
		j.LastError,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func GetJobByID(db *sql.DB, id int64) (*job.Job, error) {
	row := db.QueryRow(
		`SELECT id, command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error
         FROM jobs WHERE id=?`, id,
	)
	var j job.Job
	var state, schedStr string
	var lastErr sql.NullString
	if err := row.Scan(&j.ID, &j.Command, &state, &j.Attempts, &j.MaxRetries,
		&schedStr, &j.CreatedAt, &j.UpdatedAt, &lastErr); err != nil {
		return nil, err
	}
	j.State = job.JobState(state)
	j.ScheduledAt, _ = time.Parse(time.RFC3339, schedStr)
	if lastErr.Valid {
		j.LastError = lastErr.String
	}
	return &j, nil
}

func PullPendingJob(db *sql.DB) (*job.Job, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)

	row := tx.QueryRow(`SELECT id, command, state, attempts, max_retries, scheduled_at, created_at, updated_at, last_error
		FROM jobs
		WHERE state = ? AND scheduled_at <= ?
		ORDER BY id
		LIMIT 1`, string(job.Pending), now)

	var j job.Job
	var state, schedStr string
	var lastErr sql.NullString
	if err := row.Scan(&j.ID, &j.Command, &state, &j.Attempts, &j.MaxRetries,
		&schedStr, &j.CreatedAt, &j.UpdatedAt, &lastErr); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNoJob
		}
		return nil, err
	}

	j.State = job.JobState(state)
	j.ScheduledAt, _ = time.Parse(time.RFC3339, schedStr)
	if lastErr.Valid {
		j.LastError = lastErr.String
	}

	_, err = tx.Exec(`UPDATE jobs SET state=?, updated_at=? WHERE id=?`, string(job.Running), now, j.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	j.State = job.Running
	j.UpdatedAt = time.Now().UTC()
	return &j, nil
}

func UpdateJob(db *sql.DB, j *job.Job) error {
	_, err := db.Exec(`UPDATE jobs SET state=?, attempts=?, scheduled_at=?, updated_at=?, last_error=? WHERE id=?`,
		string(j.State), j.Attempts,
		j.ScheduledAt.Format(time.RFC3339),
		j.UpdatedAt.Format(time.RFC3339),
		j.LastError, j.ID,
	)
	return err
}

func DeleteJob(db *sql.DB, id int64) error {
	_, err := db.Exec(`DELETE FROM jobs WHERE id = ?`, id)
	return err
}

func MoveToDead(db *sql.DB, j *job.Job) error {
	now := time.Now().UTC()
	_, err := db.Exec(`INSERT INTO dead_jobs(orig_id, command, attempts, max_retries, created_at, failed_at, last_error)
        VALUES(?,?,?,?,?,?,?)`,
		j.ID, j.Command, j.Attempts, j.MaxRetries,
		j.CreatedAt.Format(time.RFC3339), now.Format(time.RFC3339), j.LastError,
	)
	if err != nil {
		return err
	}
	return DeleteJob(db, j.ID)
}

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
		var st, schedStr string
		var lastErr sql.NullString

		if err := rows.Scan(&j.ID, &j.Command, &st, &j.Attempts, &j.MaxRetries,
			&schedStr, &j.CreatedAt, &j.UpdatedAt, &lastErr); err != nil {
			return nil, err
		}

		j.State = job.JobState(st)
		j.ScheduledAt, _ = time.Parse(time.RFC3339, schedStr)
		if lastErr.Valid {
			j.LastError = lastErr.String
		} else {
			j.LastError = ""
		}
		out = append(out, j)
	}
	return out, nil
}

type DeadJob struct {
	ID         int64
	OrigID     sql.NullInt64
	Command    string
	Attempts   int
	MaxRetries int
	CreatedAt  time.Time
	FailedAt   time.Time
	LastError  sql.NullString
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


func RetryDeadJob(db *sql.DB, deadJobID int) error {
	// Fetch dead job
	row := db.QueryRow(`SELECT orig_id, command, max_retries FROM dead_jobs WHERE id = ?`, deadJobID)

	var origID sql.NullInt64
	var cmd string
	var maxRetries int

	if err := row.Scan(&origID, &cmd, &maxRetries); err != nil {
		return fmt.Errorf("dead job id %d not found: %w", deadJobID, err)
	}

	// If orig_id not stored, assign new ID (auto)
	insert := `
	INSERT INTO jobs (command, state, attempts, max_retries, scheduled_at, created_at, updated_at)
	VALUES (?, 'pending', 0, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`
	_, err := db.Exec(insert, cmd, maxRetries)
	if err != nil {
		return fmt.Errorf("requeue failed: %w", err)
	}

	// Remove from dead queue
	_, _ = db.Exec(`DELETE FROM dead_jobs WHERE id = ?`, deadJobID)

	return nil
}



func FlushPending(db *sql.DB) error {
	_, err := db.Exec(`DELETE FROM jobs WHERE state = ?`, "pending")
	return err
}

func FlushDead(db *sql.DB) error {
	_, err := db.Exec(`DELETE FROM dead_jobs`)
	return err
}

func FlushAll(db *sql.DB) error {
	_, err := db.Exec(`DELETE FROM jobs`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`DELETE FROM dead_jobs`)
	return err
}
