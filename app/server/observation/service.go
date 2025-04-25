package observation

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	formatJSON = "json"
	formatHTML = "html"
)

var _ utils.Service = (*serviceImpl)(nil)

// serviceImpl represents the HTTP service implementation
type serviceImpl struct {
	storage   Storage
	server    *http.Server
	listener  net.Listener
	logger    *zap.Logger
	templates *template.Template
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

// handleHomePage serves the main HTML page with links to all other endpoints
func (s *serviceImpl) handleHomePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Define the data for the template
	type Link struct {
		Name        string
		URL         string
		Description string
	}

	data := struct {
		Title string
		Links []Link
	}{
		Title: "Query Observation Service",
		Links: []Link{
			{
				Name:        "List Incoming Queries",
				URL:         "/api/queries/incoming/list",
				Description: "List all incoming queries with pagination",
			},
			{
				Name:        "List Running Incoming Queries",
				URL:         "/api/queries/incoming/running",
				Description: "List all currently running incoming queries",
			},
			{
				Name:        "List Outgoing Queries",
				URL:         "/api/queries/outgoing/list",
				Description: "List all outgoing queries with pagination",
			},
			{
				Name:        "List Running Outgoing Queries",
				URL:         "/api/queries/outgoing/running",
				Description: "List all currently running outgoing queries",
			},
			{
				Name:        "List Similar Outgoing Queries with Different Stats",
				URL:         "/api/queries/outgoing/similar_with_different_stats",
				Description: "Find groups of similar outgoing queries with different row counts",
			},
		},
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := s.templates.ExecuteTemplate(w, "templates/home.html", data); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleListIncomingQueries handles GET requests to list incoming queries
func (s *serviceImpl) handleListIncomingQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	queryParams := r.URL.Query()
	format := queryParams.Get("format")

	// Get limit parameter (default to 50 if not provided)
	limit := 50

	if limitStr := queryParams.Get("limit"); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}

		limit = parsedLimit
	}

	// Get offset parameter (default to 0 if not provided)
	offset := 0

	if offsetStr := queryParams.Get("offset"); offsetStr != "" {
		parsedOffset, err := strconv.Atoi(offsetStr)
		if err != nil || parsedOffset < 0 {
			http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
			return
		}

		offset = parsedOffset
	}

	// Get state parameter (optional filter)
	var stateParam *QueryState

	if stateStr := queryParams.Get("state"); stateStr != "" {
		state := QueryState(stateStr)
		stateParam = &state
	}

	// Call the storage method
	queries, err := s.storage.ListIncomingQueries(stateParam, limit, offset)
	if err != nil {
		http.Error(w, "Failed to list incoming queries", http.StatusInternalServerError)
		return
	}

	// If format is JSON or not specified, return JSON
	if format == formatJSON || format == "" {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		return
	}

	// Otherwise, render HTML
	if format == formatHTML {
		s.renderIncomingQueriesHTML(w, r, queries, limit, offset)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// renderIncomingQueriesHTML renders incoming queries as HTML with pagination
func (s *serviceImpl) renderIncomingQueriesHTML(w http.ResponseWriter, r *http.Request, queries []*IncomingQuery, limit, offset int) {
	// Data for the template
	data := struct {
		Queries     []*IncomingQuery
		Limit       int
		Offset      int
		NextOffset  int
		PrevOffset  int
		HasPrev     bool
		HasNext     bool
		StateFilter string
		QueryCount  int // Added for debugging
	}{
		Queries:    queries,
		Limit:      limit,
		Offset:     offset,
		NextOffset: offset + limit,
		PrevOffset: offset - limit,
		HasPrev:    offset > 0,
		HasNext:    len(queries) >= limit,
		QueryCount: len(queries), // For debugging
	}

	// Get state from query params if it exists
	if stateParam := r.URL.Query().Get("state"); stateParam != "" {
		data.StateFilter = stateParam
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Add debugging output to the page
	debug := fmt.Sprintf("<!-- Debug info: queries=%d, limit=%d, offset=%d -->",
		len(queries), limit, offset)

	if _, err := w.Write([]byte(debug)); err != nil {
		s.logger.Error("writer failed", zap.Error(err))
		http.Error(w, "Failed to render template: "+err.Error(), http.StatusInternalServerError)

		return
	}

	if err := s.templates.ExecuteTemplate(w, "templates/incoming_queries.html", data); err != nil {
		s.logger.Error("Template rendering failed", zap.Error(err))
		http.Error(w, "Failed to render template: "+err.Error(), http.StatusInternalServerError)

		return
	}
}

// handleListRunningIncomingQueries handles GET requests to list running incoming queries
func (s *serviceImpl) handleListRunningIncomingQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	format := r.URL.Query().Get("format")
	state := QueryStateRunning

	// Use a large limit to get all running queries
	queries, err := s.storage.ListIncomingQueries(&state, 1000, 0)
	if err != nil {
		http.Error(w, "Failed to list running incoming queries", http.StatusInternalServerError)
		return
	}

	// If format is JSON or not specified, return JSON
	if format == formatJSON || format == "" {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		return
	}

	// Otherwise, render HTML
	if format == formatHTML {
		s.renderIncomingQueriesHTML(w, r, queries, 1000, 0)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// handleListOutgoingQueries handles GET requests to list outgoing queries
//
//nolint:gocyclo
func (s *serviceImpl) handleListOutgoingQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	queryParams := r.URL.Query()
	format := queryParams.Get("format")

	// Get limit parameter (default to 50 if not provided)
	limit := 50

	if limitStr := queryParams.Get("limit"); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}

		limit = parsedLimit
	}

	// Get offset parameter (default to 0 if not provided)
	offset := 0

	if offsetStr := queryParams.Get("offset"); offsetStr != "" {
		parsedOffset, err := strconv.Atoi(offsetStr)
		if err != nil || parsedOffset < 0 {
			http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
			return
		}

		offset = parsedOffset
	}

	// Get state parameter (optional filter)
	var stateParam *QueryState

	if stateStr := queryParams.Get("state"); stateStr != "" {
		state := QueryState(stateStr)
		stateParam = &state
	}

	// Get incoming query ID parameter (optional filter)
	var incomingQueryIDParam *IncomingQueryID

	if incomingQueryIDStr := queryParams.Get("incoming_query_id"); incomingQueryIDStr != "" {
		incomingQueryID, err := strconv.ParseUint(incomingQueryIDStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid incoming_query_id parameter", http.StatusBadRequest)
			return
		}

		id := IncomingQueryID(incomingQueryID)
		incomingQueryIDParam = &id
	}

	// Call the storage method
	queries, err := s.storage.ListOutgoingQueries(incomingQueryIDParam, stateParam, limit, offset)
	if err != nil {
		http.Error(w, "Failed to list outgoing queries", http.StatusInternalServerError)
		return
	}

	// If format is JSON or not specified, return JSON
	if format == formatJSON || format == "" {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		return
	}

	// Otherwise, render HTML
	if format == formatHTML {
		s.renderOutgoingQueriesHTML(w, r, queries, limit, offset, incomingQueryIDParam)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// renderOutgoingQueriesHTML renders outgoing queries as HTML with pagination
func (s *serviceImpl) renderOutgoingQueriesHTML(
	w http.ResponseWriter,
	r *http.Request,
	queries []*OutgoingQuery,
	limit, offset int,
	incomingQueryID *IncomingQueryID) {
	// Data for the template
	data := struct {
		Queries         []*OutgoingQuery
		Limit           int
		Offset          int
		NextOffset      int
		PrevOffset      int
		HasPrev         bool
		HasNext         bool
		StateFilter     string
		IncomingQueryID *IncomingQueryID
	}{
		Queries:         queries,
		Limit:           limit,
		Offset:          offset,
		NextOffset:      offset + limit,
		PrevOffset:      offset - limit,
		HasPrev:         offset > 0,
		HasNext:         len(queries) >= limit,
		IncomingQueryID: incomingQueryID,
	}

	// Get state from query params if it exists
	if stateParam := r.URL.Query().Get("state"); stateParam != "" {
		data.StateFilter = stateParam
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := s.templates.ExecuteTemplate(w, "templates/outgoing_queries.html", data); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleListRunningOutgoingQueries handles GET requests to list running outgoing queries
func (s *serviceImpl) handleListRunningOutgoingQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	format := r.URL.Query().Get("format")
	state := QueryStateRunning

	// Use a large limit to get all running queries
	queries, err := s.storage.ListOutgoingQueries(nil, &state, 1000, 0)
	if err != nil {
		http.Error(w, "Failed to list running outgoing queries", http.StatusInternalServerError)
		return
	}

	// If format is JSON or not specified, return JSON
	if format == formatJSON || format == "" {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		return
	}

	// Otherwise, render HTML
	if format == formatHTML {
		s.renderOutgoingQueriesHTML(w, r, queries, 1000, 0, nil)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// handleListSimilarOutgoingQueriesWithDifferentStats handles GET requests to find similar outgoing queries with different rows_read
func (s *serviceImpl) handleListSimilarOutgoingQueriesWithDifferentStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	format := r.URL.Query().Get("format")

	similarQueryGroups, err := s.storage.ListSimilarOutgoingQueriesWithDifferentStats(s.logger)
	if err != nil {
		http.Error(w, "Failed to find similar outgoing queries with different stats", http.StatusInternalServerError)
		return
	}

	// If format is JSON or not specified, return JSON
	if format == formatJSON || format == "" {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(similarQueryGroups); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		return
	}

	// Otherwise, render HTML
	if format == formatHTML {
		s.renderSimilarOutgoingQueriesHTML(w, similarQueryGroups)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// renderSimilarOutgoingQueriesHTML renders similar outgoing queries with different stats as HTML
func (s *serviceImpl) renderSimilarOutgoingQueriesHTML(w http.ResponseWriter, queryGroups [][]*OutgoingQuery) {
	// Data for the template
	data := struct {
		QueryGroups [][]*OutgoingQuery
		GroupCount  int
	}{
		QueryGroups: queryGroups,
		GroupCount:  len(queryGroups),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := s.templates.ExecuteTemplate(w, "templates/similar_outgoing_queries.html", data); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// NewService creates a new observation service instance.
func NewService(
	logger *zap.Logger,
	cfg *config.TObservationConfig,
	storage Storage,
) (utils.Service, error) {
	// Load templates
	templates, err := getTemplates()
	if err != nil {
		return nil, fmt.Errorf("failed to load templates: %w", err)
	}

	var s = &serviceImpl{
		logger:    logger,
		storage:   storage,
		templates: templates,
	}

	mux := http.NewServeMux()

	// Serve static assets
	mux.Handle("/assets/", http.StripPrefix("/assets/", s.requestLoggerMiddleware(getAssetHandler())))

	// Register home page handler
	mux.Handle(
		"/",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleHomePage)),
	)

	// Register incoming queries handlers
	mux.Handle(
		"/api/queries/incoming/list",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleListIncomingQueries)),
	)
	mux.Handle(
		"/api/queries/incoming/running",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleListRunningIncomingQueries)),
	)

	// Register outgoing queries handlers
	mux.Handle(
		"/api/queries/outgoing/list",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleListOutgoingQueries)),
	)
	mux.Handle(
		"/api/queries/outgoing/running",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleListRunningOutgoingQueries)),
	)
	mux.Handle(
		"/api/queries/outgoing/similar_with_different_stats",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleListSimilarOutgoingQueriesWithDifferentStats)),
	)

	// Create listener
	addr := common.EndpointToString(cfg.Server.GetEndpoint())

	s.logger.Info("starting HTTP server", zap.String("address", addr))

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
