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
		workerCmd(db,q, os.Args[2:])
	case "jobs":
		jobsCmd(db)
	case "dlq":
		dlqCmd(db, os.Args[2:])

	case "flush":
	flushCmd(q, os.Args[2:])
	case "config":
    configCmd(db, os.Args[2:])
	case "status":
    statusCmd(db, q)
	case "list":
    listJobsCmd(db, os.Args[2:])


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
func workerCmd(db *sql.DB,q *queue.Queue, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: queuectl worker start|stop [--concurrency N]")
		return
	}

	switch args[0] {
	case "start":
		flags := flag.NewFlagSet("worker start", flag.ExitOnError)
		concurrency := flags.Int("concurrency", 1, "number of worker goroutines")
		_ = flags.Parse(args[1:])

		worker.Start(db,q, *concurrency)

	case "stop":
		fmt.Println("Sending stop signal to workers...")
		if err := worker.SignalStop(); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("Workers will stop gracefully.")
	default:
		fmt.Println("Unknown worker command. Use: start | stop")
	}
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

func dlqCmd(db *sql.DB, args []string) {
	if len(args) == 0 {
		fmt.Println("usage: queuectl dlq [list|retry <id>]")
		return
	}

	switch args[0] {
	case "list":
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
			fmt.Printf("id=%d orig=%v cmd=%s attempts=%d failed_at=%s\n",
				d.ID, d.OrigID, d.Command, d.Attempts, d.FailedAt.Format(time.RFC3339))
		}

	case "retry":
		if len(args) < 2 {
			fmt.Println("usage: queuectl dlq retry <dead_job_id>")
			return
		}

		id, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("invalid job id:", args[1])
			return
		}

		if err := storage.RetryDeadJob(db, id); err != nil {
			fmt.Printf("retry failed: %v\n", err)
			return
		}

		fmt.Printf("retried dead job %d\n", id)

	default:
		fmt.Println("usage: queuectl dlq [list|retry <id>]")
	}
}




func flushCmd(q *queue.Queue, args []string) {
	if len(args) < 1 {
		fmt.Println("usage: queuectl flush [pending|dead|all]")
		return
	}

	kind := args[0]

	if err := q.Flush(kind); err != nil {
		fmt.Printf("flush failed: %v\n", err)
		return
	}

	fmt.Printf("flushed %s jobs\n", kind)
}
func configCmd(db *sql.DB, args []string) {
	if len(args) == 0 {
		// list all
		cfg, err := storage.ConfigList(db)
		if err != nil {
			log.Fatalf("config list: %v", err)
		}
		if len(cfg) == 0 {
			fmt.Println("no config values set")
			return
		}
		for k, v := range cfg {
			fmt.Printf("%s = %s\n", k, v)
		}
		return
	}

	switch args[0] {
	case "get":
		if len(args) < 2 {
			fmt.Println("usage: queuectl config get <key>")
			return
		}
		val, err := storage.ConfigGet(db, args[1])
		if err != nil {
			log.Fatalf("config get: %v", err)
		}
		if val == "" {
			fmt.Printf("%s not set\n", args[1])
			return
		}
		fmt.Println(val)


	case "set":
		if len(args) < 3 {
			fmt.Println("usage: queuectl config set <key> <value>")
			return
		}
		if err := storage.ConfigSet(db, args[1], args[2]); err != nil {
			log.Fatalf("config set: %v", err)
		}
		fmt.Printf("%s set to %s\n", args[1], args[2])

	default:
		fmt.Println("usage: queuectl config [get|set]")
	}
}

func statusCmd(db *sql.DB, q *queue.Queue) {
    counts := map[job.JobState]int{}
    states := []job.JobState{job.Pending, job.Running, job.Failed, job.Completed, job.Dead}

    for _, s := range states {
        n, err := storage.CountJobsByState(db, s)
        if err != nil {
            log.Fatalf("count jobs: %v", err)
        }
        counts[s] = n
    }

    next, err := storage.GetNextScheduledJob(db)
    if err != nil && err != sql.ErrNoRows {
        log.Fatalf("fetch next job: %v", err)
    }

    cfg, _ := storage.ConfigList(db)

    fmt.Println("=== Queue Status ===")
    fmt.Printf("Pending: %d\n", counts[job.Pending])
    fmt.Printf("Running: %d\n", counts[job.Running])
    fmt.Printf("Failed: %d\n", counts[job.Failed])
    fmt.Printf("Completed: %d\n", counts[job.Completed])
    fmt.Printf("Dead (DLQ): %d\n", counts[job.Dead])
    if next != nil {
        fmt.Printf("Next Scheduled Job: ID=%d at %s\n", next.ID, next.ScheduledAt.Format(time.RFC3339))
    } else {
        fmt.Println("Next Scheduled Job: none")
    }

    fmt.Println("\n=== Config ===")
    for k, v := range cfg {
        fmt.Printf("%s = %s\n", k, v)
    }

    fmt.Println("\n=== Workers ===")
    workers, _ := worker.GetAllWorkerStatus(db)
    for _, w := range workers {
        fmt.Printf("worker-%d: state=%s job_id=%d updated=%s\n", w.ID, w.State, w.CurrentJobID, w.UpdatedAt.Format(time.RFC3339))
    }
}



func listJobsCmd(db *sql.DB, args []string) {
    flags := flag.NewFlagSet("list", flag.ExitOnError)
    state := flags.String("state", "", "filter jobs by state (pending, running, failed, completed)")
    _ = flags.Parse(args)

    // If no state given, show all
    if *state == "" {
        jobsCmd(db)
        return
    }

    js, err := storage.GetJobsByState(db, job.JobState(*state))
    if err != nil {
        log.Fatalf("get jobs: %v", err)
    }
    if len(js) == 0 {
        fmt.Printf("No jobs in state: %s\n", *state)
        return
    }

    fmt.Printf("=== %s ===\n", *state)
    for _, j := range js {
        fmt.Printf(
            "id=%d cmd=%s attempts=%d scheduled_at=%s updated_at=%s\n",
            j.ID, j.Command, j.Attempts,
            j.ScheduledAt.Format(time.RFC3339),
            j.UpdatedAt.Format(time.RFC3339),
        )
    }
}
