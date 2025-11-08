package worker

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"queuectl/internal/queue"
	"syscall"
	"time"
)



const stopFile = ".queuectl_workers.stop"

// check if stop file exists
func shouldStop() bool {
	_, err := os.Stat(stopFile)
	return err == nil
}

// create stop file
func SignalStop() error {
	return os.WriteFile(stopFile, []byte("stop"), 0644)
}

// remove stop file (cleanup)
func ClearStopSignal() {
	_ = os.Remove(stopFile)
}




func Start(db *sql.DB, q *queue.Queue, concurrency int) {
    fmt.Println("Starting worker.........Press Ctrl + C to stop")
    ClearStopSignal()

    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

    for i := 0; i < concurrency; i++ {
        workerID := i + 1
        _ = UpdateWorkerStatus(db, workerID, "idle", 0)

        go func(id int) {
            for {
                if shouldStop() {
                    fmt.Printf("Worker %d stopping...\n", id)
                    _ = UpdateWorkerStatus(db, id, "stopped", 0)
                    return
                }

                select {
                case <-stop:
                    fmt.Printf("Worker %d received interrupt\n", id)
                    _ = UpdateWorkerStatus(db, id, "stopped", 0)
                    return
                default:
                    job, err := q.Pull()
                    if err != nil {
                        time.Sleep(500 * time.Millisecond)
                        continue
                    }
                    if job == nil {
                        time.Sleep(300 * time.Millisecond)
                        continue
                    }

                    _ = UpdateWorkerStatus(db, id, "running", job.ID)

                    err = runJob(job.Command)
                    if err != nil {
                        log.Printf("job %d failed: %v", job.ID, err)
                        _ = q.Reject(job, err.Error())
                    } else {
                        log.Printf("job %d completed", job.ID)
                        _ = q.Ack(job)
                    }

                    _ = UpdateWorkerStatus(db, id, "idle", 0)
                }
            }
        }(workerID)
    }

    <-stop
    fmt.Println("Shutting down all workers")
    _ = SignalStop()
    time.Sleep(1 * time.Second)
}




func runJob (cmd string) error{

    c := exec.Command("sh", "-c", cmd)

    output, err := c.CombinedOutput()

    if err != nil {
        return fmt.Errorf("error: %v | output: %s", err, output)
    }

    fmt.Printf("job output: %s\n", output)
    return nil

}