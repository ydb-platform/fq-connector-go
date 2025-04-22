package observation

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

// QueryState represents the possible states of a query
type QueryState string

const (
	QueryStateRunning   QueryState = "running"
	QueryStateFinished  QueryState = "finished"
	QueryStateCancelled QueryState = "cancelled"
)

var _ Storage = (*storageSQLite)(nil)

// storageSQLite handles storing and retrieving query data
type storageSQLite struct {
	db   *sql.DB
	path string
}

// initialize creates the necessary tables if they don't exist
func (s *storageSQLite) initialize() error {
	// Create the queries table
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS queries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		database_name TEXT NOT NULL,
		database_endpoint TEXT NOT NULL,
		data_source_kind TEXT NOT NULL,
		query_text TEXT,
		query_args TEXT,
		created_at TIMESTAMP NOT NULL,
		finished_at TIMESTAMP,
		rows_read INTEGER NOT NULL DEFAULT 0,
		bytes_read INTEGER NOT NULL DEFAULT 0,
		state TEXT NOT NULL,
		error TEXT
	);
	
	CREATE INDEX IF NOT EXISTS idx_queries_state ON queries(state);
	CREATE INDEX IF NOT EXISTS idx_queries_created_at ON queries(created_at);
	CREATE INDEX IF NOT EXISTS idx_queries_query_text ON queries(query_text);
	CREATE INDEX IF NOT EXISTS idx_queries_query_args ON queries(query_args);
	CREATE INDEX IF NOT EXISTS idx_queries_database_name ON queries(database_name);
	CREATE INDEX IF NOT EXISTS idx_queries_datasource_kind ON queries(data_source_kind);
	`

	_, err := s.db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	return nil
}

// Close closes the database connection
func (s *storageSQLite) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// CreateQuery creates a new query record with auto-generated ID
func (s *storageSQLite) CreateQuery(dsi *api_common.TGenericDataSourceInstance) (QueryID, error) {
	query := &Query{
		DatabaseName:     dsi.Database,
		DatabaseEndpoint: common.EndpointToString(dsi.Endpoint),
		DataSourceKind:   dsi.Kind.String(),
		CreatedAt:        time.Now().UTC(),
		RowsRead:         0,
		BytesRead:        0,
		State:            QueryStateRunning,
	}

	result, err := s.db.Exec(
		"INSERT INTO queries (database_name, database_endpoint, data_source_kind, created_at, rows_read, bytes_read, state) VALUES (?, ?, ?, ?, ?, ?, ?)",
		query.DatabaseName, query.DatabaseEndpoint, query.DataSourceKind, query.CreatedAt, query.RowsRead, query.BytesRead, string(query.State),
	)
	if err != nil {
		return 0, fmt.Errorf("creating query: %w", err)
	}

	// Get the auto-generated ID
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("retrieving generated ID: %w", err)
	}
	query.ID = QueryID(id)

	return query.ID, nil
}

// SetQueryDetails sets the query text and arguments
func (s *storageSQLite) SetQueryDetails(id QueryID, queryText, queryArgs string) error {
	result, err := s.db.Exec(
		"UPDATE queries SET query_text = ?, query_args = ? WHERE id = ?",
		queryText, queryArgs, id,
	)
	if err != nil {
		return fmt.Errorf("setting query details: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("query not found: %d", id)
	}

	return nil
}

// GetQuery retrieves a query by its ID
func (s *storageSQLite) GetQuery(id QueryID) (*Query, error) {
	var query Query
	var finishedAt sql.NullTime
	var queryText, queryArgs, errorMsg sql.NullString

	err := s.db.QueryRow(
		`SELECT id, database_name, database_endpoint, data_source_kind, 
		 query_text, query_args, created_at, finished_at, rows_read, bytes_read, state, error 
		 FROM queries WHERE id = ?`,
		id,
	).Scan(
		&query.ID, &query.DatabaseName, &query.DatabaseEndpoint, &query.DataSourceKind,
		&queryText, &queryArgs, &query.CreatedAt, &finishedAt, &query.RowsRead, &query.BytesRead,
		&query.State, &errorMsg,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("query not found: %d", id)
		}
		return nil, fmt.Errorf("retrieving query: %w", err)
	}

	if queryText.Valid {
		query.QueryText = queryText.String
	}

	if queryArgs.Valid {
		query.QueryArgs = queryArgs.String
	}

	if errorMsg.Valid {
		query.Error = errorMsg.String
	}

	if finishedAt.Valid {
		query.FinishedAt = &finishedAt.Time
	}

	return &query, nil
}

// ListQueries retrieves a list of queries with optional filtering
func (s *storageSQLite) ListQueries(state *QueryState, limit, offset int) ([]*Query, error) {
	var querySQL string
	var args []interface{}

	if state == nil {
		querySQL = `
			SELECT id, database_name, database_endpoint, data_source_kind, 
			query_text, query_args, created_at, finished_at, rows_read, bytes_read, state, error 
			FROM queries ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []interface{}{limit, offset}
	} else {
		querySQL = `
			SELECT id, database_name, database_endpoint, data_source_kind, 
			query_text, query_args, created_at, finished_at, rows_read, bytes_read, state, error 
			FROM queries WHERE state = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []interface{}{string(*state), limit, offset}
	}

	rows, err := s.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("listing queries: %w", err)
	}
	defer rows.Close()

	var queries []*Query
	for rows.Next() {
		var query Query
		var finishedAt sql.NullTime
		var queryText, queryArgs, errorMsg sql.NullString

		if err := rows.Scan(
			&query.ID, &query.DatabaseName, &query.DatabaseEndpoint, &query.DataSourceKind,
			&queryText, &queryArgs, &query.CreatedAt, &finishedAt, &query.RowsRead, &query.BytesRead,
			&query.State, &errorMsg,
		); err != nil {
			return nil, fmt.Errorf("scanning query: %w", err)
		}

		if queryText.Valid {
			query.QueryText = queryText.String
		}

		if queryArgs.Valid {
			query.QueryArgs = queryArgs.String
		}

		if errorMsg.Valid {
			query.Error = errorMsg.String
		}

		if finishedAt.Valid {
			query.FinishedAt = &finishedAt.Time
		}

		queries = append(queries, &query)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating queries: %w", err)
	}

	return queries, nil
}

// UpdateQueryProgress updates the number of rows and bytes read for a query
func (s *storageSQLite) UpdateQueryProgress(id QueryID, rowsRead, bytesRead int64) error {
	result, err := s.db.Exec(
		"UPDATE queries SET rows_read = ?, bytes_read = ? WHERE id = ?",
		rowsRead, bytesRead, id,
	)
	if err != nil {
		return fmt.Errorf("updating query progress: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("query not found: %d", id)
	}

	return nil
}

// FinishQuery marks a query as finished
func (s *storageSQLite) FinishQuery(id QueryID, rowsRead, bytesRead int64) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE queries SET state = ?, finished_at = ?, rows_read = ?, bytes_read = ? WHERE id = ?",
		string(QueryStateFinished), finishedAt, rowsRead, bytesRead, id,
	)
	if err != nil {
		return fmt.Errorf("marking query as finished: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("query not found: %d", id)
	}

	return nil
}

// CancelQuery marks a query as cancelled with error message and resource usage data
func (s *storageSQLite) CancelQuery(id QueryID, errorMsg string, rowsRead, bytesRead int64) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE queries SET state = ?, finished_at = ?, error = ?, rows_read = ?, bytes_read = ? WHERE id = ?",
		string(QueryStateCancelled), finishedAt, errorMsg, rowsRead, bytesRead, id,
	)
	if err != nil {
		return fmt.Errorf("cancelling query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("query not found: %d", id)
	}

	return nil
}

// DeleteQuery removes a query from the database
func (s *storageSQLite) DeleteQuery(id QueryID) error {
	result, err := s.db.Exec("DELETE FROM queries WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("deleting query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("query not found: %d", id)
	}

	return nil
}

// GetRunningQueries gets all queries that are currently running
func (s *storageSQLite) GetRunningQueries() ([]*Query, error) {
	state := QueryStateRunning
	return s.ListQueries(&state, 1000, 0)
}

// FindSimilarQueriesWithDifferentUsage finds groups of queries with identical text and args but different resource usage
func (s *storageSQLite) FindSimilarQueriesWithDifferentUsage() ([][]*Query, error) {
	// First, find groups of queries with the same text and args
	findSimilarSQL := `
	WITH query_groups AS (
		SELECT 
			query_text, 
			query_args, 
			COUNT(*) as count,
			COUNT(DISTINCT rows_read) as distinct_rows,
			COUNT(DISTINCT bytes_read) as distinct_bytes
		FROM 
			queries
		WHERE 
			query_text IS NOT NULL AND
			query_text != '' AND
			state != ?
		GROUP BY 
			query_text, query_args
		HAVING 
			COUNT(*) > 1 AND
			(distinct_rows > 1 OR distinct_bytes > 1)
	)
	SELECT 
		query_text, query_args
	FROM 
		query_groups
	LIMIT 100;
	`

	rows, err := s.db.Query(findSimilarSQL, string(QueryStateRunning))
	if err != nil {
		return nil, fmt.Errorf("finding similar queries: %w", err)
	}
	defer rows.Close()

	// Store query text and args pairs
	type queryKey struct {
		text string
		args string
	}

	var keys []queryKey
	for rows.Next() {
		var text, args string
		if err := rows.Scan(&text, &args); err != nil {
			return nil, fmt.Errorf("scanning query key: %w", err)
		}
		keys = append(keys, queryKey{text, args})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating query keys: %w", err)
	}

	// For each unique query text+args, fetch all matching queries
	var result [][]*Query
	for _, key := range keys {
		fetchSQL := `
		SELECT 
			id, database_name, database_endpoint, data_source_kind, 
			query_text, query_args, created_at, finished_at, rows_read, bytes_read, state, error
		FROM 
			queries
		WHERE 
			query_text = ? AND
			query_args = ? AND
			state != ?
		ORDER BY 
			created_at DESC
		`

		qrows, err := s.db.Query(fetchSQL, key.text, key.args, string(QueryStateRunning))
		if err != nil {
			return nil, fmt.Errorf("fetching query group: %w", err)
		}

		var group []*Query
		for qrows.Next() {
			var query Query
			var finishedAt sql.NullTime
			var queryText, queryArgs, errorMsg sql.NullString

			if err := qrows.Scan(
				&query.ID, &query.DatabaseName, &query.DatabaseEndpoint, &query.DataSourceKind,
				&queryText, &queryArgs, &query.CreatedAt, &finishedAt, &query.RowsRead, &query.BytesRead,
				&query.State, &errorMsg,
			); err != nil {
				qrows.Close()
				return nil, fmt.Errorf("scanning query: %w", err)
			}

			if queryText.Valid {
				query.QueryText = queryText.String
			}

			if queryArgs.Valid {
				query.QueryArgs = queryArgs.String
			}

			if errorMsg.Valid {
				query.Error = errorMsg.String
			}

			if finishedAt.Valid {
				query.FinishedAt = &finishedAt.Time
			}

			group = append(group, &query)
		}
		qrows.Close()

		if err := qrows.Err(); err != nil {
			return nil, fmt.Errorf("iterating query group: %w", err)
		}

		// Only add the group if we have multiple queries
		if len(group) > 1 {
			result = append(result, group)
		}
	}

	return result, nil
}

// newStorageSQLite creates a new QueryStorage instance
func newStorageSQLite(cfg *config.TObservationConfig_TStorage_TSQLite) (Storage, error) {
	db, err := sql.Open("sqlite3", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite database: %w", err)
	}

	// Set pragmas for better performance
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA cache_size=5000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=30000000000",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return nil, fmt.Errorf("setting pragma %s: %w", pragma, err)
		}
	}

	storage := &storageSQLite{
		db:   db,
		path: cfg.Path,
	}

	if err := storage.initialize(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize: %w", err)
	}

	return storage, nil
}
