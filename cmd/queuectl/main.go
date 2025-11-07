package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"queuectl/internal/job"
	"queuectl/internal/queue"
	"queuectl/internal/storage"
	"queuectl/internal/worker"
)

const dbPath = "queue.db"

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	cmd := os.Args[1]

	db, err := storage.OpenDB(dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	q := queue.NewQueue(db)

	switch cmd {
	case "enqueue":
		enqueueCmd(q, os.Args[2:])
	case "worker":
		workerCmd(q, os.Args[2:])
	case "jobs":
		jobsCmd(db)
	case "dlq":
		dlqCmd(db)
	case "retry":
		retryCmd(db, os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Println(`queuectl <cmd> [args]
commands:
  enqueue <command> [--retries N]   enqueue a job
  worker                            run a single worker (blocks)
  jobs                              list active jobs
  dlq                               list dead jobs
  retry <dead_job_id>               (not implemented) placeholder
`)
}

func enqueueCmd(q *queue.Queue, args []string) {
	flags := flag.NewFlagSet("enqueue", flag.ExitOnError)
	retries := flags.Int("retries", 3, "max retries")
	_ = flags.Parse(args)

	rest := flags.Args()
	if len(rest) < 1 {
		fmt.Println("enqueue requires a command string")
		return
	}
	cmd := strings.Join(rest, " ")
	j, err := q.Push(cmd, *retries)
	if err != nil {
		log.Fatalf("push: %v", err)
	}
	fmt.Printf("enqueued id=%d cmd=%s\n", j.ID, j.Command)
}

// simple worker loop that executes commands using a fake handler.
// Replace runJobHandler with real business logic.
func workerCmd(q *queue.Queue, args []string) {
    flags := flag.NewFlagSet("worker", flag.ExitOnError)
    concurrency := flags.Int("concurrency", 1, "number of worker goroutines")
    _ = flags.Parse(args)

    worker.Start(q, *concurrency)
}


func jobsCmd(db *sql.DB) {
	activeStates := []string{"pending", "running", "failed", "completed"}
	for _, s := range activeStates {
		js, err := storage.GetJobsByState(db, job.JobState(s))
		if err != nil {
			log.Fatalf("get jobs: %v", err)
		}
		if len(js) == 0 {
			continue
		}
		fmt.Printf("=== %s ===\n", s)
		for _, j := range js {
			fmt.Printf("id=%d cmd=%s attempts=%d scheduled_at=%s updated_at=%s\n", j.ID, j.Command, j.Attempts, j.ScheduledAt.Format(time.RFC3339), j.UpdatedAt.Format(time.RFC3339))
		}
	}
}

func dlqCmd(db *sql.DB) {
	ds, err := storage.ListDeadJobs(db)
	if err != nil {
		log.Fatalf("list dead jobs: %v", err)
	}
	if len(ds) == 0 {
		fmt.Println("no dead jobs")
		return
	}
	fmt.Println("=== dead jobs ===")
	for _, d := range ds {
		fmt.Printf("id=%d orig_id=%v cmd=%s attempts=%d failed_at=%s last_error=%v\n", d.ID, d.OrigID, d.Command, d.Attempts, d.FailedAt.Format(time.RFC3339), d.LastError)
	}
}

func retryCmd(db *sql.DB, args []string) {
    if len(args) < 1 {
        fmt.Println("retry <dead_job_id>")
        return
    }

    id64, err := strconv.ParseInt(args[0], 10, 64)
    if err != nil {
        fmt.Println("invalid id")
        return
    }

    q := queue.NewQueue(db)
    j, err := q.Retry(id64)
    if err != nil {
        fmt.Printf("retry failed: %v\n", err)
        return
    }

    fmt.Printf("job re-enqueued: id=%d cmd=%s\n", j.ID, j.Command)
}
