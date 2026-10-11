package queue

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Engine is a thread-safe, in-memory queue engine supporting at-least-once delivery,
// visibility timeouts, idempotency, delayed scheduling, and dead-letter queues.
type Engine struct {
	mu             sync.RWMutex
	jobs           map[string]*Job
	queues         map[string][]string // queue_name -> ordered list of job_ids
	idempotencyMap map[string]string   // idempotency_key -> job_id
	dlq            map[string]*Job     // job_id -> dead lettered job
	stopCh         chan struct{}
}

// NewEngine constructs and starts an in-memory queue engine with automatic lease sweeping.
func NewEngine() *Engine {
	e := &Engine{
		jobs:           make(map[string]*Job),
		queues:         make(map[string][]string),
		idempotencyMap: make(map[string]string),
		dlq:            make(map[string]*Job),
		stopCh:         make(chan struct{}),
	}
	go e.startReaperLoop()
	return e
}

// Close stops background sweepers.
func (e *Engine) Close() {
	close(e.stopCh)
}

// Enqueue adds a job to the queue, checking idempotency.
func (e *Engine) Enqueue(job *Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Idempotency check
	if job.IdempotencyKey != "" {
		if existingID, exists := e.idempotencyMap[job.IdempotencyKey]; exists {
			existingJob := e.jobs[existingID]
			if existingJob != nil && (existingJob.State == StateCompleted || existingJob.State == StateProcessing || existingJob.State == StatePending) {
				return ErrJobAlreadyExists
			}
		}
		e.idempotencyMap[job.IdempotencyKey] = job.ID
	}

	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now

	if job.NextRunAt.After(now) {
		job.State = StateDelayed
	} else {
		job.State = StatePending
	}

	e.jobs[job.ID] = job
	e.queues[job.Queue] = append(e.queues[job.Queue], job.ID)
	return nil
}

// Schedule queues a job to be processed after a specified delay duration.
func (e *Engine) Schedule(job *Job, delay time.Duration) error {
	job.NextRunAt = time.Now().UTC().Add(delay)
	return e.Enqueue(job)
}

// Dequeue fetches the next eligible job and assigns a visibility lease to the worker.
func (e *Engine) Dequeue(ctx context.Context, queueName, workerID string) (*Job, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	jobIDs, exists := e.queues[queueName]
	if !exists || len(jobIDs) == 0 {
		return nil, ErrQueueEmpty
	}

	now := time.Now().UTC()
	for i, id := range jobIDs {
		job := e.jobs[id]
		if job == nil {
			continue
		}

		// Check if job is eligible: (Pending or Delayed whose time arrived)
		isDelayedReady := job.State == StateDelayed && (job.NextRunAt.Before(now) || job.NextRunAt.Equal(now))
		isPendingReady := job.State == StatePending && (job.NextRunAt.Before(now) || job.NextRunAt.Equal(now))

		if isPendingReady || isDelayedReady {
			// Acquire lease
			job.State = StateProcessing
			job.WorkerID = workerID
			job.UpdatedAt = now
			job.VisibilityDeadline = now.Add(job.VisibilityTimeout)

			// Remove from pending queue slice
			e.queues[queueName] = append(jobIDs[:i], jobIDs[i+1:]...)
			return job, nil
		}
	}

	return nil, ErrQueueEmpty
}

// Ack acknowledges successful completion of the job.
func (e *Engine) Ack(jobID, workerID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, exists := e.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}

	now := time.Now().UTC()
	job.State = StateCompleted
	job.CompletedAt = &now
	job.UpdatedAt = now
	return nil
}

// Nack handles worker failure, triggering exponential backoff or routing to DLQ.
func (e *Engine) Nack(jobID, workerID, errReason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, exists := e.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}

	now := time.Now().UTC()
	job.RetryCount++
	job.LastError = errReason
	job.UpdatedAt = now

	// Check if max retries exceeded -> Dead Letter Queue
	if job.RetryCount >= job.MaxRetries {
		job.State = StateDeadLettered
		e.dlq[job.ID] = job
		return ErrMaxRetriesExceeded
	}

	// Calculate exponential backoff: base * 2^retries + jitter
	base := 1.0 // 1 second
	backoffSec := base * math.Pow(2, float64(job.RetryCount))
	jitter := rand.Float64() * 0.5
	delayDuration := time.Duration((backoffSec + jitter) * float64(time.Second))

	job.State = StateDelayed
	job.NextRunAt = now.Add(delayDuration)
	e.queues[job.Queue] = append(e.queues[job.Queue], job.ID)
	return nil
}

// Heartbeat extends the visibility timeout for long-running jobs.
func (e *Engine) Heartbeat(jobID, workerID string, extendBy time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, exists := e.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	if job.State != StateProcessing || job.WorkerID != workerID {
		return ErrLeaseExpired
	}

	job.VisibilityDeadline = time.Now().UTC().Add(extendBy)
	job.UpdatedAt = time.Now().UTC()
	return nil
}

// ReapExpiredLeases reclaims jobs from dead/crashed workers whose visibility deadline passed.
func (e *Engine) ReapExpiredLeases() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UTC()
	reclaimed := 0

	for _, job := range e.jobs {
		if job.State == StateProcessing && now.After(job.VisibilityDeadline) {
			// Worker crashed or timed out! Return to pending queue
			job.State = StatePending
			job.WorkerID = ""
			job.LastError = "visibility timeout expired (worker lease lost)"
			job.UpdatedAt = now
			e.queues[job.Queue] = append(e.queues[job.Queue], job.ID)
			reclaimed++
		}
	}
	return reclaimed
}

// GetDLQ returns all jobs currently inside the Dead-Letter Queue.
func (e *Engine) GetDLQ() []*Job {
	e.mu.RLock()
	defer e.mu.RUnlock()

	res := make([]*Job, 0, len(e.dlq))
	for _, job := range e.dlq {
		res = append(res, job)
	}
	return res
}

// RetryDLQJob re-enqueues a dead-lettered job by resetting its retries.
func (e *Engine) RetryDLQJob(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, exists := e.dlq[jobID]
	if !exists {
		return ErrJobNotFound
	}

	delete(e.dlq, jobID)
	job.RetryCount = 0
	job.State = StatePending
	job.LastError = ""
	job.NextRunAt = time.Now().UTC()
	job.UpdatedAt = time.Now().UTC()
	e.queues[job.Queue] = append(e.queues[job.Queue], job.ID)
	return nil
}

func (e *Engine) startReaperLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.ReapExpiredLeases()
		}
	}
}
