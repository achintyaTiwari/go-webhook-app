package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"bytes"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// LogPayload is the struct to deserialize json log payloads 
type LogPayload struct {
	UserID    int64   `json:"user_id"`
	Total     float64 `json:"total"`
	Title     string  `json:"title"`
	Meta      Metadata `json:"meta"`
	Completed bool `json:"completed"`
}

// Metadata contains logins and phone numbers
type Metadata struct {
	Logins []Login `json:"logins"`
	PhoneNumbers PhoneNumbers `json:"phone_numbers"`
}

// Login contains login time and IP 
type Login struct {
	Time time.Time `json:"time"`
	IP string `json:"ip"`
}

// PhoneNumbers contains home and mobile numbers
type PhoneNumbers struct {
	Home string `json:"home"`
	Mobile string `json:"mobile"` 
}

var (
	batchSize, _ = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	batchInterval, _ = strconv.Atoi(os.Getenv("BATCH_INTERVAL"))
	postURL = os.Getenv("POST_ENDPOINT")
	logPayloadChannel = make(chan LogPayload, batchSize)
	logger *zap.Logger
)

func main() {

	// Initialize logger

	logger, _ = zap.NewProduction()
	defer func() {
		if err := logger.Sync(); err != nil {
			logger.Error("Failed to flush logs", zap.Error(err))
		}  
	 }()

	// Create router and define routes
	 
	r := chi.NewRouter()

	r.Get("/healthz", healthCheckHandler)

	r.Post("/log", handleLog)

	// Log startup message

	logger.Info("Server started", 
		zap.String("batch_size", os.Getenv("BATCH_SIZE")),
		zap.String("batch_interval", os.Getenv("BATCH_INTERVAL")),
		zap.String("post_endpoint", os.Getenv("POST_ENDPOINT")),
	)

	// Start log batch processor goroutine

	go processLogBatch()

	// Start server

	if err := http.ListenAndServe(":8080", r); err != nil {
        logger.Fatal("Failed to start server",
          zap.Error(err))
  	}
}



// Health check handler

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if _, err := w.Write([]byte("OK")); err != nil {
		logger.Error("Failed to write",
		   zap.Error(err))
	}
}


// Handle new log requests

func handleLog(w http.ResponseWriter, r *http.Request) {
	// Decode JSON payload
	var payload LogPayload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Send payload to channel
	logPayloadChannel <- payload

	// Write accepted response
	w.WriteHeader(http.StatusAccepted)

	// Log receipt
	logger.Info("Log payload received",
		zap.Int64("user_id", payload.UserID),
		zap.Float64("total", payload.Total), 	
		zap.String("title", payload.Title),
	)
}



// Batch processor loop

func processLogBatch() {
	
	// Batching ticker
	tick := time.NewTicker(time.Second * time.Duration(batchInterval))

	// Current log batch
	var logBatch []LogPayload

	// Wait group for batch sends
	var wg sync.WaitGroup

	for {
		select {

		// New payload	
		case payload := <-logPayloadChannel:

			// Add payload to current batch
			logBatch = append(logBatch, payload)

			// If batch is full, send it
			if len(logBatch) == batchSize {
				wg.Add(1)
				go sendBatch(&wg, logBatch)
				logBatch = make([]LogPayload, 0) 
			}

		// Batch interval elapsed	
		case <-tick.C:

			// Send remaining batch 
			if len(logBatch) > 0 {
				wg.Add(1)
				go sendBatch(&wg, logBatch)
				logBatch = make([]LogPayload, 0)
			}
		}	
	}
}

// Attempt batch send with retries

func sendBatch(wg *sync.WaitGroup, batch []LogPayload) {
	
	// Marlowe batch send
	defer wg.Done()
	
	// Serialize batch to JSON
	data, _ := json.Marshal(batch)

	// Create request
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, postURL, bytes.NewBuffer(data))
	
	// Track send time
	start := time.Now()	
	var status int	
	
	// Send loop
	for try := 1; try <= 3; try++ {
		logger.Info("Sending batch", 
			zap.Int("batch_size", len(batch)),
			zap.Int("try", try))
		

		// Send batch	
		resp, err := http.DefaultClient.Do(req)
		
		// Check result
		if resp != nil {
			status = resp.StatusCode
			resp.Body.Close()
		}
		
		// Success criteria
		if err == nil && (status == 200 || status == 202) {
			break 
		}

		// Retry loguc
		
		if try < 3 {
			logger.Error("Batch send failed, retrying", 
				zap.Int("batch_size", len(batch)),
				zap.Int("status_code", status),
				zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		
		// Send failure
		logger.Fatal("Failed to send batch after 3 retries, exiting",
			zap.Int("batch_size", len(batch)),
			zap.Int("status_code", status),
			zap.Error(err))
	}
	
	duration := time.Since(start)
	
	// Log batch send duration
	logger.Info("Batch sent",
		zap.Int("batch_size", len(batch)),
		zap.Int("status_code", status),
		zap.Duration("duration", duration),
	)
}