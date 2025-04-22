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
				Name:        "List Similar Queries with Different Stats",
				URL:         "/api/queries/similar_with_different_stats",
				Description: "Find groups of similar queries with different resource usage",
			},
		},
	}

	// HTML template
	const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>{{.Title}}</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            line-height: 1.6;
            color: #333;
        }
        h1 {
            color: #2c3e50;
            margin-bottom: 30px;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }
        .link-container {
            margin-bottom: 20px;
            padding: 15px;
            background-color: #f9f9f9;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .link-container:hover {
            background-color: #f0f0f0;
        }
        .link-name {
            font-size: 18px;
            font-weight: bold;
            color: #3498db;
            text-decoration: none;
        }
        .link-name:hover {
            text-decoration: underline;
        }
        .link-description {
            margin-top: 5px;
            color: #666;
        }
    </style>
</head>
<body>
    <h1>{{.Title}}</h1>
    {{range .Links}}
    <div class="link-container">
        <a href="{{.URL}}" class="link-name">{{.Name}}</a>
        <div class="link-description">{{.Description}}</div>
    </div>
    {{end}}
</body>
</html>
`

	tmpl, err := template.New("home").Parse(htmlTemplate)
	if err != nil {
		http.Error(w, "Failed to parse template", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
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
	if format == "json" || format == "" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
		return
	}

	// Otherwise, render HTML
	if format == "html" {
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
	}{
		Queries:    queries,
		Limit:      limit,
		Offset:     offset,
		NextOffset: offset + limit,
		PrevOffset: offset - limit,
		HasPrev:    offset > 0,
		HasNext:    len(queries) >= limit,
	}

	// Get state from query params if it exists
	if stateParam := r.URL.Query().Get("state"); stateParam != "" {
		data.StateFilter = stateParam
	}

	// HTML template for incoming queries
	const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Incoming Queries</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            line-height: 1.6;
            color: #333;
        }
        h1 {
            color: #2c3e50;
            margin-bottom: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tr:hover {
            background-color: #f0f0f0;
        }
        .pagination {
            margin-top: 20px;
        }
        .pagination a {
            display: inline-block;
            padding: 8px 16px;
            text-decoration: none;
            background-color: #3498db;
            color: white;
            border-radius: 5px;
            margin-right: 10px;
        }
        .pagination a:hover {
            background-color: #2980b9;
        }
        .pagination a.disabled {
            background-color: #cccccc;
            cursor: not-allowed;
        }
        .back-link {
            margin-bottom: 20px;
            display: block;
        }
        .state-running {
            color: blue;
            font-weight: bold;
        }
        .state-finished {
            color: green;
        }
        .state-canceled {
            color: red;
        }
    </style>
</head>
<body>
    <a href="/" class="back-link">← Back to Home</a>
    <h1>Incoming Queries</h1>
    
    {{if .Queries}}
    <table>
        <tr>
            <th>ID</th>
            <th>Data Source Kind</th>
            <th>Rows Read</th>
            <th>Bytes Read</th>
            <th>State</th>
            <th>Created At</th>
            <th>Finished At</th>
            <th>Error</th>
        </tr>
        {{range .Queries}}
        <tr>
            <td>{{.ID}}</td>
            <td>{{.DataSourceKind}}</td>
            <td>{{.RowsRead}}</td>
            <td>{{.BytesRead}}</td>
            <td class="state-{{.State}}">{{.State}}</td>
            <td>{{.CreatedAt}}</td>
            <td>{{if .FinishedAt}}{{.FinishedAt}}{{else}}-{{end}}</td>
            <td>{{if .Error}}{{.Error}}{{else}}-{{end}}</td>
        </tr>
        {{end}}
    </table>
    
    <div class="pagination">
        {{if .HasPrev}}
        <a href="?format=html&limit={{.Limit}}&offset={{.PrevOffset}}{{if .StateFilter}}&state={{.StateFilter}}{{end}}">Previous</a>
        {{else}}
        <a href="#" class="disabled">Previous</a>
        {{end}}
        
        {{if .HasNext}}
        <a href="?format=html&limit={{.Limit}}&offset={{.NextOffset}}{{if .StateFilter}}&state={{.StateFilter}}{{end}}">Next</a>
        {{else}}
        <a href="#" class="disabled">Next</a>
        {{end}}
    </div>
    {{else}}
    <p>No queries found.</p>
    {{end}}
</body>
</html>
`

	tmpl, err := template.New("incoming_queries").Parse(htmlTemplate)
	if err != nil {
		http.Error(w, "Failed to parse template", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
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
	if format == "json" || format == "" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
		return
	}

	// Otherwise, render HTML
	if format == "html" {
		s.renderIncomingQueriesHTML(w, r, queries, 1000, 0)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// handleListOutgoingQueries handles GET requests to list outgoing queries
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
	if format == "json" || format == "" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
		return
	}

	// Otherwise, render HTML
	if format == "html" {
		s.renderOutgoingQueriesHTML(w, r, queries, limit, offset, incomingQueryIDParam)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// renderOutgoingQueriesHTML renders outgoing queries as HTML with pagination
func (s *serviceImpl) renderOutgoingQueriesHTML(w http.ResponseWriter, r *http.Request, queries []*OutgoingQuery, limit, offset int, incomingQueryID *IncomingQueryID) {
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

	// HTML template for outgoing queries
	const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Outgoing Queries</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            line-height: 1.6;
            color: #333;
        }
        h1 {
            color: #2c3e50;
            margin-bottom: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tr:hover {
            background-color: #f0f0f0;
        }
        .pagination {
            margin-top: 20px;
        }
        .pagination a {
            display: inline-block;
            padding: 8px 16px;
            text-decoration: none;
            background-color: #3498db;
            color: white;
            border-radius: 5px;
            margin-right: 10px;
        }
        .pagination a:hover {
            background-color: #2980b9;
        }
        .pagination a.disabled {
            background-color: #cccccc;
            cursor: not-allowed;
        }
        .back-link {
            margin-bottom: 20px;
            display: block;
        }
        .state-running {
            color: blue;
            font-weight: bold;
        }
        .state-finished {
            color: green;
        }
        .state-canceled {
            color: red;
        }
        .query-text {
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .query-text:hover {
            white-space: normal;
            overflow: visible;
        }
    </style>
</head>
<body>
    <a href="/" class="back-link">← Back to Home</a>
    <h1>Outgoing Queries</h1>
    
    {{if .IncomingQueryID}}
    <p><strong>Filtered by Incoming Query ID:</strong> {{.IncomingQueryID}}</p>
    {{end}}
    
    {{if .Queries}}
    <table>
        <tr>
            <th>ID</th>
            <th>Incoming Query ID</th>
            <th>Database</th>
            <th>Endpoint</th>
            <th>Query Text</th>
            <th>State</th>
            <th>Created At</th>
            <th>Finished At</th>
            <th>Error</th>
        </tr>
        {{range .Queries}}
        <tr>
            <td>{{.ID}}</td>
            <td>
                <a href="/api/queries/outgoing/list?format=html&incoming_query_id={{.IncomingQueryID}}">
                    {{.IncomingQueryID}}
                </a>
            </td>
            <td>{{.DatabaseName}}</td>
            <td>{{.DatabaseEndpoint}}</td>
            <td class="query-text">{{.QueryText}}</td>
            <td class="state-{{.State}}">{{.State}}</td>
            <td>{{.CreatedAt}}</td>
            <td>{{if .FinishedAt}}{{.FinishedAt}}{{else}}-{{end}}</td>
            <td>{{if .Error}}{{.Error}}{{else}}-{{end}}</td>
        </tr>
        {{end}}
    </table>
    
    <div class="pagination">
        {{if .HasPrev}}
        <a href="?format=html&limit={{.Limit}}&offset={{.PrevOffset}}{{if .StateFilter}}&state={{.StateFilter}}{{end}}{{if .IncomingQueryID}}&incoming_query_id={{.IncomingQueryID}}{{end}}">Previous</a>
        {{else}}
        <a href="#" class="disabled">Previous</a>
        {{end}}
        
        {{if .HasNext}}
        <a href="?format=html&limit={{.Limit}}&offset={{.NextOffset}}{{if .StateFilter}}&state={{.StateFilter}}{{end}}{{if .IncomingQueryID}}&incoming_query_id={{.IncomingQueryID}}{{end}}">Next</a>
        {{else}}
        <a href="#" class="disabled">Next</a>
        {{end}}
    </div>
    {{else}}
    <p>No queries found.</p>
    {{end}}
</body>
</html>
`

	tmpl, err := template.New("outgoing_queries").Parse(htmlTemplate)
	if err != nil {
		http.Error(w, "Failed to parse template", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
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
	if format == "json" || format == "" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queries); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
		return
	}

	// Otherwise, render HTML
	if format == "html" {
		s.renderOutgoingQueriesHTML(w, r, queries, 1000, 0, nil)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// handleListSimilarQueriesWithDifferentStats handles GET requests to find similar queries with different stats
func (s *serviceImpl) handleListSimilarQueriesWithDifferentStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	format := r.URL.Query().Get("format")

	similarQueryGroups, err := s.storage.ListSimilarIncomingQueriesWithDifferentStats()
	if err != nil {
		http.Error(w, "Failed to find similar queries with different stats", http.StatusInternalServerError)
		return
	}

	// If format is JSON or not specified, return JSON
	if format == "json" || format == "" {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(similarQueryGroups); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
		return
	}

	// Otherwise, render HTML
	if format == "html" {
		s.renderSimilarQueriesHTML(w, similarQueryGroups)
		return
	}

	// Unsupported format
	http.Error(w, "Unsupported format", http.StatusBadRequest)
}

// renderSimilarQueriesHTML renders similar queries with different stats as HTML
func (s *serviceImpl) renderSimilarQueriesHTML(w http.ResponseWriter, queryGroups [][]*IncomingQuery) {
	// Data for the template
	data := struct {
		QueryGroups [][]*IncomingQuery
		GroupCount  int
	}{
		QueryGroups: queryGroups,
		GroupCount:  len(queryGroups),
	}
	// HTML template for similar queries (continued)
	const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Similar Queries with Different Stats</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            line-height: 1.6;
            color: #333;
        }
        h1, h2 {
            color: #2c3e50;
        }
        h1 {
            margin-bottom: 20px;
        }
        h2 {
            margin-top: 30px;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tr:hover {
            background-color: #f0f0f0;
        }
        .back-link {
            margin-bottom: 20px;
            display: block;
        }
        .group {
            margin-bottom: 40px;
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stats-diff {
            color: #e74c3c;
            font-weight: bold;
        }
        .no-groups {
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 5px;
            text-align: center;
            font-size: 18px;
            color: #7f8c8d;
        }
    </style>
</head>
<body>
    <a href="/" class="back-link">← Back to Home</a>
    <h1>Similar Queries with Different Stats</h1>
    
    {{if gt .GroupCount 0}}
        <p>Found {{.GroupCount}} groups of similar queries with different resource usage.</p>
        
        {{range $groupIndex, $group := .QueryGroups}}
            <div class="group">
                <h2>Query Group {{$groupIndex | inc}}</h2>
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Data Source Kind</th>
                        <th class="stats-diff">Rows Read</th>
                        <th class="stats-diff">Bytes Read</th>
                        <th>State</th>
                        <th>Created At</th>
                        <th>Finished At</th>
                    </tr>
                    {{range $query := $group}}
                    <tr>
                        <td>{{$query.ID}}</td>
                        <td>{{$query.DataSourceKind}}</td>
                        <td class="stats-diff">{{$query.RowsRead}}</td>
                        <td class="stats-diff">{{$query.BytesRead}}</td>
                        <td>{{$query.State}}</td>
                        <td>{{$query.CreatedAt}}</td>
                        <td>{{if $query.FinishedAt}}{{$query.FinishedAt}}{{else}}-{{end}}</td>
                    </tr>
                    {{end}}
                </table>
            </div>
        {{end}}
    {{else}}
        <div class="no-groups">
            <p>No similar queries with different resource usage found.</p>
        </div>
    {{end}}
</body>
</html>
`

	// Create a function map to increment the group index for display
	funcMap := template.FuncMap{
		"inc": func(i int) int {
			return i + 1
		},
	}

	tmpl, err := template.New("similar_queries").Funcs(funcMap).Parse(htmlTemplate)
	if err != nil {
		http.Error(w, "Failed to parse template", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
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
	var (
		s = &serviceImpl{
			logger:  logger,
			storage: storage,
		}
	)

	mux := http.NewServeMux()

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

	// Register similar queries handler
	mux.Handle(
		"/api/queries/similar_with_different_stats",
		s.requestLoggerMiddleware(http.HandlerFunc(s.handleListSimilarQueriesWithDifferentStats)),
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
