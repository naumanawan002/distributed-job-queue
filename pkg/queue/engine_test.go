package queue

import (
	"context"
	"testing"
	"time"
)

func TestEngine_BasicLifecycle(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	job := NewJob("emails", []byte(`{"to":"alice@example.com"}`))
	if err := engine.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Dequeue
	leased, err := engine.Dequeue(context.Background(), "emails", "worker-1")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if leased.ID != job.ID || leased.State != StateProcessing || leased.WorkerID != "worker-1" {
		t.Errorf("unexpected leased job state: %+v", leased)
	}

	// Ack
	if err := engine.Ack(leased.ID, "worker-1"); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if engine.jobs[job.ID].State != StateCompleted {
		t.Errorf("expected StateCompleted, got %s", engine.jobs[job.ID].State)
	}
}

func TestEngine_VisibilityTimeoutAndRedelivery(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	job := NewJob("payments", []byte(`{"charge":100}`))
	// 50ms visibility timeout for rapid test
	job.VisibilityTimeout = 50 * time.Millisecond
	_ = engine.Enqueue(job)

	// Worker 1 leases job
	leased1, _ := engine.Dequeue(context.Background(), "payments", "crashed-worker")
	if leased1 == nil {
		t.Fatal("failed to lease job")
	}

	// Worker 1 crashes (no Ack, no Nack)!
	// Wait 70ms for visibility timeout to expire
	time.Sleep(70 * time.Millisecond)

	// Trigger lease reaper
	reclaimed := engine.ReapExpiredLeases()
	if reclaimed != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", reclaimed)
	}

	// Worker 2 should now be able to pick up the abandoned job!
	leased2, err := engine.Dequeue(context.Background(), "payments", "surviving-worker")
	if err != nil {
		t.Fatalf("worker 2 failed to lease reclaimed job: %v", err)
	}
	if leased2.ID != job.ID {
		t.Errorf("expected job %s, got %s", job.ID, leased2.ID)
	}

	// Clean ack
	_ = engine.Ack(leased2.ID, "surviving-worker")
}

func TestEngine_IdempotencyDeduplication(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	j1 := NewJob("orders", []byte(`{"order_id":123}`))
	j1.IdempotencyKey = "unique-order-key-123"
	if err := engine.Enqueue(j1); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	// Duplicate job with same idempotency key
	j2 := NewJob("orders", []byte(`{"order_id":123,"duplicate":true}`))
	j2.IdempotencyKey = "unique-order-key-123"
	err := engine.Enqueue(j2)
	if err != ErrJobAlreadyExists {
		t.Errorf("expected ErrJobAlreadyExists, got %v", err)
	}
}

func TestEngine_ExponentialBackoffAndDLQ(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	job := NewJob("webhooks", []byte(`{"url":"https://failing-webhook.com"}`))
	job.MaxRetries = 2
	_ = engine.Enqueue(job)

	// Attempt 1: Nack
	leased, _ := engine.Dequeue(context.Background(), "webhooks", "w1")
	err1 := engine.Nack(leased.ID, "w1", "500 Internal Server Error")
	if err1 != nil {
		t.Fatalf("first nack failed: %v", err1)
	}
	if engine.jobs[job.ID].RetryCount != 1 || engine.jobs[job.ID].State != StateDelayed {
		t.Errorf("expected RetryCount 1 and StateDelayed, got count=%d, state=%s",
			engine.jobs[job.ID].RetryCount, engine.jobs[job.ID].State)
	}

	// Fast-forward delay time for testing
	engine.mu.Lock()
	engine.jobs[job.ID].NextRunAt = time.Now().UTC().Add(-time.Second)
	engine.mu.Unlock()

	// Attempt 2: Second Nack (exhausts retries)
	leased2, _ := engine.Dequeue(context.Background(), "webhooks", "w2")
	err2 := engine.Nack(leased2.ID, "w2", "Connection Refused")
	if err2 != ErrMaxRetriesExceeded {
		t.Fatalf("expected ErrMaxRetriesExceeded, got %v", err2)
	}

	// Job must now reside in DLQ
	dlqJobs := engine.GetDLQ()
	if len(dlqJobs) != 1 || dlqJobs[0].ID != job.ID {
		t.Fatalf("expected job in DLQ, found: %v", dlqJobs)
	}
	if dlqJobs[0].State != StateDeadLettered {
		t.Errorf("expected StateDeadLettered, got %s", dlqJobs[0].State)
	}

	// Test DLQ Replay
	errRetry := engine.RetryDLQJob(job.ID)
	if errRetry != nil {
		t.Fatalf("failed to retry DLQ job: %v", errRetry)
	}
	if len(engine.GetDLQ()) != 0 {
		t.Error("DLQ should be empty after re-enqueuing job")
	}
}
