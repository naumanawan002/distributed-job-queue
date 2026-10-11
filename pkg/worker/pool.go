package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nauman/distributed-job-queue/pkg/queue"
)

// HandlerFunc is the user callback that executes the actual work of a job.
type HandlerFunc func(ctx context.Context, job *queue.Job) error

// Pool coordinates concurrent worker goroutines consuming from a queue.
type Pool struct {
	engine      *queue.Engine
	queueName   string
	concurrency int
	handler     HandlerFunc
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewPool creates a worker pool instance.
func NewPool(engine *queue.Engine, queueName string, concurrency int, handler HandlerFunc) *Pool {
	return &Pool{
		engine:      engine,
		queueName:   queueName,
		concurrency: concurrency,
		handler:     handler,
		stopCh:      make(chan struct{}),
	}
}

// Start launches worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.concurrency; i++ {
		workerID := fmt.Sprintf("worker-%s-%d", p.queueName, i+1)
		p.wg.Add(1)
		go p.workerLoop(workerID)
	}
}

// Stop gracefully stops all workers and waits for in-flight jobs to finish.
func (p *Pool) Stop(timeout time.Duration) {
	close(p.stopCh)
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[INFO] Worker pool for queue %q stopped cleanly.", p.queueName)
	case <-time.After(timeout):
		log.Printf("[WARN] Worker pool stop timed out after %v.", timeout)
	}
}

func (p *Pool) workerLoop(workerID string) {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		default:
			job, err := p.engine.Dequeue(context.Background(), p.queueName, workerID)
			if err != nil {
				// Queue empty or no eligible jobs; back off briefly
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Process job with heartbeating
			p.processJob(workerID, job)
		}
	}
}

func (p *Pool) processJob(workerID string, job *queue.Job) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Heartbeat ticker extends visibility timeout every half-period
	heartbeatInterval := job.VisibilityTimeout / 2
	if heartbeatInterval > time.Second {
		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					_ = p.engine.Heartbeat(job.ID, workerID, job.VisibilityTimeout)
				}
			}
		}()
	}

	// Execute user job handler
	err := p.handler(ctx, job)
	if err != nil {
		_ = p.engine.Nack(job.ID, workerID, err.Error())
	} else {
		_ = p.engine.Ack(job.ID, workerID)
	}
}
