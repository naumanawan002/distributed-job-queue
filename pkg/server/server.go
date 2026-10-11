package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/nauman/distributed-job-queue/pkg/queue"
)

// Server exposes an HTTP interface to enqueue jobs and manage the Dead-Letter Queue.
type Server struct {
	engine *queue.Engine
	mux    *http.ServeMux
}

type EnqueueRequest struct {
	Queue             string `json:"queue"`
	Payload           string `json:"payload"`
	IdempotencyKey    string `json:"idempotency_key,omitempty"`
	DelaySeconds      int    `json:"delay_seconds,omitempty"`
	MaxRetries        int    `json:"max_retries,omitempty"`
	VisibilityTimeout int    `json:"visibility_timeout_sec,omitempty"`
}

func NewServer(engine *queue.Engine) *Server {
	s := &Server{
		engine: engine,
		mux:    http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/jobs", s.handleJobs)
	s.mux.HandleFunc("/dlq", s.handleDLQ)
	s.mux.HandleFunc("/dlq/", s.handleDLQItem)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	if req.Queue == "" {
		http.Error(w, "queue name is required", http.StatusBadRequest)
		return
	}

	job := queue.NewJob(req.Queue, []byte(req.Payload))
	job.IdempotencyKey = req.IdempotencyKey
	if req.MaxRetries > 0 {
		job.MaxRetries = req.MaxRetries
	}
	if req.VisibilityTimeout > 0 {
		job.VisibilityTimeout = time.Duration(req.VisibilityTimeout) * time.Second
	}

	var err error
	if req.DelaySeconds > 0 {
		err = s.engine.Schedule(job, time.Duration(req.DelaySeconds)*time.Second)
	} else {
		err = s.engine.Enqueue(job)
	}

	if err != nil {
		if err == queue.ErrJobAlreadyExists {
			respondJSON(w, http.StatusConflict, map[string]string{
				"error":   "duplicate_job",
				"message": "job with this idempotency key already active or completed",
			})
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]any{
		"status":  "enqueued",
		"job_id":  job.ID,
		"state":   job.State,
		"next_at": job.NextRunAt,
	})
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dlqJobs := s.engine.GetDLQ()
	respondJSON(w, http.StatusOK, map[string]any{
		"dlq_count": len(dlqJobs),
		"jobs":      dlqJobs,
	})
}

func (s *Server) handleDLQItem(w http.ResponseWriter, r *http.Request) {
	// Path format: /dlq/{id}/retry
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) == 3 && parts[2] == "retry" && r.Method == http.MethodPost {
		jobID := parts[1]
		err := s.engine.RetryDLQJob(jobID)
		if err != nil {
			if err == queue.ErrJobNotFound {
				http.Error(w, "job not found in DLQ", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		respondJSON(w, http.StatusOK, map[string]string{
			"status":  "re-enqueued",
			"job_id":  jobID,
			"message": "job returned from DLQ to pending state with reset retries",
		})
		return
	}

	http.Error(w, "not found", http.StatusNotFound)
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
