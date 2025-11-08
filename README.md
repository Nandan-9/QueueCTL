````markdown
# QueueCTL

QueueCTL is a CLI-based background job queue system built in Go. It supports persistent job storage, multiple workers, automatic retries with exponential backoff, and a dead-letter queue (DLQ) for failed jobs.

---

## 📌 Features

- CLI application for managing jobs
- Persistent SQLite storage for jobs and workers
- Multiple concurrent worker support
- Retry mechanism with configurable exponential backoff
- Dead Letter Queue for jobs exceeding max retries
- Configuration management via database
- Minimal testing script for validating core flows

---

## 🛠 Setup Instructions

1. **Clone the repository**:

```bash
git clone <repo_url>
cd QueueCTL
````

2. **Install dependencies**:

```bash
go mod tidy
```

3. **Build the CLI**:

```bash
go build -o queuectl
```

4. **Run the CLI**:

```bash
./queuectl <command> [args]
```

The SQLite database `queue.db` will be created automatically in the project root.

---

## ⚡ Usage Examples

### Enqueue a job

```bash
./queuectl enqueue "echo Hello World"
```

Output:

```
enqueued id=1 cmd=echo Hello World
```

### Start a worker

```bash
./queuectl worker start --concurrency 2
```

* Starts 2 worker goroutines.
* Workers continuously pick up jobs and process them.

### Check queue status

```bash
./queuectl status
```

Sample output:

```
=== Queue Status ===
Pending: 0
Running: 1
Failed: 2
Completed: 5
Dead (DLQ): 0
Next Scheduled Job: none

=== Workers ===
worker-1: state=running job_id=3
worker-2: state=idle job_id=0
```

### List active jobs

```bash
./queuectl jobs
```

* Shows jobs by state (`failed`, `completed`).

### Dead Letter Queue

```bash
./queuectl dlq
```

* Shows all jobs that exceeded retry limits.

---

## 🏗 Architecture Overview

### Job Lifecycle

1. **Pending** — Newly enqueued jobs.
2. **Running** — Jobs currently being processed by a worker.
3. **Failed** — Jobs that failed execution but still have retries left.
4. **Completed** — Jobs successfully executed.
5. **Dead** — Jobs that exceeded the max retry limit (DLQ).

### Data Persistence

* **SQLite DB** (`queue.db`)
* Tables:

  * `jobs` — stores job metadata and state
  * `dead_jobs` — stores jobs that moved to DLQ
  * `config` — runtime configuration
  * `workers` — optional: tracks active workers and states

### Worker Logic

* Workers run as goroutines.
* Pull jobs from the queue respecting `Pending` state.
* Retry failed jobs using exponential backoff: `delay = base^(attempts-1)` seconds.
* Move jobs to DLQ when `Attempts > max_retries`.

---

## ⚖️ Assumptions & Trade-offs

* Workers run in the same process for simplicity; no distributed queue support yet.
* Job commands are executed via shell (`sh -c`) — security risks if untrusted input.
* Worker status is tracked in-memory (and optionally persisted) for basic monitoring.
* Retry base and max retries are configurable, but no dynamic scaling of workers is implemented.
* Job deletion after completion is optional for audit purposes.

---

## 🧪 Testing Instructions

1. Run the minimal test script:

```bash
go run scripts/test_queuectl.go
```

* Enqueues sample jobs.
* Starts a worker in background.
* Prints status of jobs, completed, failed, and DLQ.

2. Verify DLQ:

```bash
./queuectl dlq
```

* Failing jobs exceeding retries should appear here.

3. Manual Testing:

* Enqueue commands that succeed (`echo Hello`) and fail (`exit 1`).
* Observe status changes using:

```bash
./queuectl status
./queuectl jobs
./queuectl dlq
```

---

## 📜 CLI Commands Overview

```text
queuectl <command> [args]

commands:
  enqueue <command> [--retries N]   enqueue a job
  worker start [--concurrency N]    start worker(s)
  status                             show queue & worker status
  jobs                               list jobs by state
  dlq                                list dead jobs
```

---

This README ensures any developer or evaluator can quickly set up, run, and verify QueueCTL with clarity on architecture, design decisions, and testing methodology.

```
```