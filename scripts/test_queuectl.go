package main

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

func run(args ...string) string {
	cmd := exec.Command("./queuectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error running %s: %v\nOutput: %s\n", strings.Join(args, " "), err, string(out))
		return string(out)
	}
	return string(out)
}

func main() {
	fmt.Println("=== Testing QueueCTL CLI ===")

	// Flush all jobs to start fresh
	fmt.Println("\n-- Flush existing jobs --")
	run("flush-all")

	// Enqueue a simple job
	fmt.Println("\n-- Enqueue job 'echo hello' --")
	out := run("enqueue", "echo hello")
	fmt.Println(out)

	// Enqueue a failing job to test retries & DLQ
	fmt.Println("\n-- Enqueue job 'exit 1' with 1 retry --")
	out = run("enqueue", "exit 1", "--retries", "1")
	fmt.Println(out)

	// Start a worker (1 goroutine)
	fmt.Println("\n-- Start worker (runs in background) --")
	go run("worker", "start", "--concurrency", "1")
	time.Sleep(1 * time.Second) // give worker time to start

	// Wait for jobs to process
	fmt.Println("\n-- Waiting 3 seconds for jobs to execute --")
	time.Sleep(3 * time.Second)

	// Check status
	fmt.Println("\n-- Queue status --")
	status := run("status")
	fmt.Println(status)

	// Check failed jobs (should include exit 1)
	fmt.Println("\n-- Jobs list --")
	jobs := run("jobs")
	fmt.Println(jobs)

	// Wait additional 3 seconds to let retry run
	fmt.Println("\n-- Waiting 3 more seconds for retry --")
	time.Sleep(3 * time.Second)

	// Check status again
	fmt.Println("\n-- Queue status after retry --")
	status = run("status")
	fmt.Println(status)

	// Check dead jobs (DLQ)
	fmt.Println("\n-- Dead jobs (DLQ) --")
	dlq := run("dlq list")
	fmt.Println(dlq)

	fmt.Println("\n=== Test Completed ===")
}
