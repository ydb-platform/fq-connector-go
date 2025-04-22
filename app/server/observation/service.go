package observation

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
	"go.uber.org/zap"
)

var _ utils.Service = (*serviceImpl)(nil)

// serviceImpl represents the HTTP service implementation
type serviceImpl struct {
	storage  Storage
	server   *http.Server
	listener net.Listener
	logger   *zap.Logger
}

// Start starts the HTTP server on the specified address
func (s *serviceImpl) Start() error {
	// Start the server using the listener
	return s.server.Serve(s.listener)
}

func (s *serviceImpl) Stop() {
	s.logger.Info("Shutting down service")

	// Create a timeout context for server shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First shutdown the HTTP server gracefully
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			s.logger.Error("Error shutting down HTTP server", zap.Error(err))
		}
	}

	// Close the listener if it exists
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Error closing listener", zap.Error(err))
		}
	}

	// Finally close the storage
	err := s.storage.Close()
	if err != nil {
		s.logger.Error("Error closing storage", zap.Error(err))
	} else {
		s.logger.Info("Storage closed successfully")
	}
}

// requestLoggerMiddleware logs information about requests
func (s *serviceImpl) requestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Create a response wrapper to capture status code
		rw := newResponseWriter(w)

		// Log request handling started
		s.logger.Info("request handling started",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.String("user_agent", r.UserAgent()),
		)

		// Process the request
		next.ServeHTTP(rw, r)

		// Calculate duration
		duration := time.Since(startTime)

		// Determine if the request was successful based on status code
		success := rw.statusCode < 400

		// Prepare common log fields
		logFields := []zap.Field{
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.Int("status", rw.statusCode),
			zap.Bool("success", success),
			zap.Duration("duration", duration),
			zap.Int("response_size", rw.size),
		}

		// Use Error level for error responses, Info level for successful responses
		if !success {
			s.logger.Error("request handling failed", logFields...)
		} else {
			s.logger.Info("request handling finished", logFields...)
		}
	})
}

// responseWriter is a wrapper around http.ResponseWriter that captures status code and response size
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int
}

// newResponseWriter creates a new responseWriter
func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK, // Default to 200 OK
	}
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

// Write captures the response size
func (rw *responseWriter) Write(b []byte) (int, error) {
	size, err := rw.ResponseWriter.Write(b)
	rw.size += size
	return size, err
}

// handleGetRunningQueries handles GET requests to retrieve running queries
func (s *serviceImpl) handleGetRunningQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queries, err := s.storage.GetRunningQueries()
	if err != nil {
		// Just return the error to the client, middleware will log it
		http.Error(w, "Failed to get running queries", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queries); err != nil {
		// Just return the error to the client, middleware will log it
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// handleFindSimilarQueriesWithDifferentUsage handles GET requests to find similar queries with different usage
func (s *serviceImpl) handleFindSimilarQueriesWithDifferentUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	similarQueries, err := s.storage.FindSimilarQueriesWithDifferentUsage()
	if err != nil {
		// Just return the error to the client, middleware will log it
		http.Error(w, "Failed to find similar queries", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(similarQueries); err != nil {
		// Just return the error to the client, middleware will log it
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// NewService creates a new observation service instance.
func NewService(logger *zap.Logger, cfg *config.TObservationConfig) (utils.Service, error) {
	var (
		s = &serviceImpl{
			logger: logger,
		}

		err error
	)

	s.storage, err = newStorageSQLite(cfg.Storage.GetSqlite())
	if err != nil {
		return nil, fmt.Errorf("new storage SQLite: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(
		"/api/queries/running",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleGetRunningQueries)),
	)
	mux.Handle(
		"/api/queries/similar",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleFindSimilarQueriesWithDifferentUsage)),
	)

	// Create listener
	addr := common.EndpointToString(cfg.Server.GetEndpoint())

	s.logger.Info("starting HTTP server", zap.String("addr", addr))

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("net listen: %w", err)
	}

	s.listener = listener

	// Create and configure the server
	s.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return s, nil
}
