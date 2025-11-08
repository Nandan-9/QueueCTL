package worker

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"queuectl/internal/queue"
	"syscall"
	"time"
)







func Start(q*queue.Queue, concurrency int){

	fmt.Println("Staring worker.........Press Ctrl + C to stop")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	for i := 0; i < concurrency; i++ {
		go func (workerID int)  {
			for {
		select {
		case <- stop:
			return

		default:

			job, err := q.Pull()

			if err != nil {
				log.Printf("pull error:%v",err)
				time.Sleep(500* time.Millisecond)
				continue
			}

			if job == nil {
				time.Sleep(300 * time.Millisecond)
				continue
			}

			err = runJob(job.Command)

			if err != nil {
				log.Printf("job %d failed: %v", job.ID, err)

			if err := q.Reject(job, err.Error()); err != nil {
				log.Printf("error rejecting job %d: %v", job.ID,err)
			}			
			
			} else {
				log.Printf("job %d completed", job.ID)
				_ = q.Ack(job)
			}
		}
	}
			
		}(i + 1)
	}
	    <-stop
    fmt.Println("Shutting down all workers")
	
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