package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nauman/distributed-job-queue/pkg/queue"
	"github.com/nauman/distributed-job-queue/pkg/server"
	"github.com/nauman/distributed-job-queue/pkg/worker"
)

func main() {
	port := flag.String("port", ":8090", "HTTP server port")
	workers := flag.Int("workers", 3, "Number of background worker goroutines")
	flag.Parse()

	log.Println("==========================================================")
	log.Println("  Distributed Job Queue & Scheduler Engine (from scratch)")
	log.Println("==========================================================")

	engine := queue.NewEngine()
	defer engine.Close()

	// Start internal worker pool consuming "default" queue
	pool := worker.NewPool(engine, "default", *workers, func(ctx context.Context, job *queue.Job) error {
		log.Printf("[WORKER] Processing Job ID: %s (Queue: %s, Attempt: %d/%d)\n",
			job.ID, job.Queue, job.RetryCount+1, job.MaxRetries)
		// Simulate fast task processing
		time.Sleep(50 * time.Millisecond)
		log.Printf("[WORKER] Successfully completed Job ID: %s\n", job.ID)
		return nil
	})
	pool.Start()

	srv := &http.Server{
		Addr:         *port,
		Handler:      server.NewServer(engine),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[INFO] Job Queue Management API listening on %s\n", *port)
		log.Println("[INFO]   POST /jobs        - Enqueue or schedule a job")
		log.Println("[INFO]   GET  /dlq         - Inspect Dead-Letter Queue")
		log.Println("[INFO]   POST /dlq/:id/retry - Re-enqueue dead-lettered job")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[INFO] Shutting down Queue server and draining worker pools...")

	pool.Stop(5 * time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	log.Println("[INFO] Queue service terminated cleanly.")
}
