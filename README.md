# QueueCTL 🚀

**QueueCTL** is a CLI-based background job queue system written in Go. It supports persistent job storage, multiple workers, automatic retries with exponential backoff, and a dead-letter queue (DLQ) for failed jobs.

---

## 📌 Key Features

- CLI application for managing background jobs
- Persistent SQLite storage for jobs and workers
- Multiple concurrent worker support
- Automatic retries with configurable exponential backoff
- Dead Letter Queue (DLQ) for jobs exceeding retry limits
- Configurable runtime parameters via database
- Minimal testing scripts for validating core flows

---

## 🛠 Setup Instructions

### 1. Clone the repository
```bash
git clone <repo_url>
cd QueueCTL


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
## 🎥 Demo Video

Watch a complete demo of QueueCTL in action:

[![QueueCTL Demo Video](https://img.youtube.com/vi/VIDEO_ID/0.jpg)](https://drive.google.com/file/d/1ENT5mELasORm8-7eha9iQOarPzYVgPke/view?usp=sharing)

*Click the image above to play the demo video.*

---

##  Note from the Developer

To the FLAM Team,  

Thank you for giving me the opportunity to build an application like **QueueCTL**. It has been an incredible journey designing and implementing this CLI-based job queue system from scratch.  

This project presented several challenges that pushed me out of my comfort zone:  
- I primarily work with **Python and JavaScript**, but I chose **Go** for this project to fully leverage its concurrency model and strong type safety.  
- Designing a **persistent, fault-tolerant job queue** with retry logic, dead-letter queue, and configurable backoff required careful planning of job lifecycle and worker management.  
- Ensuring a clean **CLI interface** while supporting multiple workers, graceful shutdown, and real-time status tracking was a rewarding challenge.  

While this may not be the final or “perfect” version, I’ve strived to deliver a **robust, functional, and maintainable solution** that demonstrates my ability to:  
- Learn new technologies quickly and apply them effectively.  
- Build complex backend systems with concurrency and persistence.  
- Write clean, modular, and testable code.  

I hope this project showcases my dedication, problem-solving skills, and ability to take initiative. I am excited about the opportunity to contribute to **FLAM** and continue building scalable, high-quality software.  

Thank you for considering my submission.

— **Devan**

```
```