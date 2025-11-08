package worker

import "sync"

type WorkerStatus struct {
	ID          int
	State       string // "idle" or "running"
	CurrentJobID int64
}

var (
	wsMu sync.Mutex
	workerStatuses = map[int]*WorkerStatus{}
)

func updateWorkerStatus(id int, state string, jobID int64) {
	wsMu.Lock()
	defer wsMu.Unlock()
	workerStatuses[id] = &WorkerStatus{
		ID: id, State: state, CurrentJobID: jobID,
	}
}

func GetWorkerStatus() []*WorkerStatus {
	wsMu.Lock()
	defer wsMu.Unlock()
	res := []*WorkerStatus{}
	for _, w := range workerStatuses {
		res = append(res, w)
	}
	return res
}
