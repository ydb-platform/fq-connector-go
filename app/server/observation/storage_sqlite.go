package observation

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ Storage = (*storageSQLite)(nil)

// storageSQLite handles storing and retrieving query data
type storageSQLite struct {
	db   *sql.DB
	path string
}

// initialize creates the necessary tables if they don't exist
func (s *storageSQLite) initialize() error {
	// Create the incoming_queries table
	createIncomingTableSQL := `
	CREATE TABLE IF NOT EXISTS incoming_queries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data_source_kind TEXT NOT NULL,
		rows_read INTEGER NOT NULL DEFAULT 0,
		bytes_read INTEGER NOT NULL DEFAULT 0,
		state TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL,
		finished_at TIMESTAMP,
		error TEXT
	);
	
	CREATE INDEX IF NOT EXISTS idx_incoming_queries_state ON incoming_queries(state);
	CREATE INDEX IF NOT EXISTS idx_incoming_queries_created_at ON incoming_queries(created_at);
	CREATE INDEX IF NOT EXISTS idx_incoming_queries_datasource_kind ON incoming_queries(data_source_kind);
	`

	// Create the outgoing_queries table with foreign key
	createOutgoingTableSQL := `
	CREATE TABLE IF NOT EXISTS outgoing_queries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		incoming_query_id INTEGER NOT NULL,
		database_name TEXT NOT NULL,
		database_endpoint TEXT NOT NULL,
		query_text TEXT,
		query_args TEXT,
		state TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL,
		finished_at TIMESTAMP,
		error TEXT,
		FOREIGN KEY (incoming_query_id) REFERENCES incoming_queries(id) ON DELETE CASCADE
	);
	
	CREATE INDEX IF NOT EXISTS idx_outgoing_queries_state ON outgoing_queries(state);
	CREATE INDEX IF NOT EXISTS idx_outgoing_queries_created_at ON outgoing_queries(created_at);
	CREATE INDEX IF NOT EXISTS idx_outgoing_queries_incoming_id ON outgoing_queries(incoming_query_id);
	CREATE INDEX IF NOT EXISTS idx_outgoing_queries_query_text ON outgoing_queries(query_text);
	CREATE INDEX IF NOT EXISTS idx_outgoing_queries_query_args ON outgoing_queries(query_args);
	CREATE INDEX IF NOT EXISTS idx_outgoing_queries_database_name ON outgoing_queries(database_name);
	`

	// Enable foreign key support
	_, err := s.db.Exec("PRAGMA foreign_keys = ON;")
	if err != nil {
		return fmt.Errorf("enabling foreign keys: %w", err)
	}

	// Create tables
	_, err = s.db.Exec(createIncomingTableSQL)
	if err != nil {
		return fmt.Errorf("creating incoming_queries table: %w", err)
	}

	_, err = s.db.Exec(createOutgoingTableSQL)
	if err != nil {
		return fmt.Errorf("creating outgoing_queries table: %w", err)
	}

	return nil
}

// CreateIncomingQuery creates a new incoming query record
func (s *storageSQLite) CreateIncomingQuery(dataSourceKind api_common.EGenericDataSourceKind) (IncomingQueryID, error) {
	query := &IncomingQuery{
		DataSourceKind: dataSourceKind.String(),
		CreatedAt:      time.Now().UTC(),
		RowsRead:       0,
		BytesRead:      0,
		State:          QueryStateRunning,
	}

	result, err := s.db.Exec(
		"INSERT INTO incoming_queries (data_source_kind, created_at, rows_read, bytes_read, state) VALUES (?, ?, ?, ?, ?)",
		query.DataSourceKind, query.CreatedAt, query.RowsRead, query.BytesRead, string(query.State),
	)
	if err != nil {
		return 0, fmt.Errorf("creating incoming query: %w", err)
	}

	// Get the auto-generated ID
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("retrieving generated ID: %w", err)
	}

	return IncomingQueryID(id), nil
}

// FinishIncomingQuery marks an incoming query as finished with final stats
func (s *storageSQLite) FinishIncomingQuery(id IncomingQueryID, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE incoming_queries SET state = ?, finished_at = ?, rows_read = ?, bytes_read = ? WHERE id = ?",
		string(QueryStateFinished), finishedAt, stats.Rows, stats.Bytes, id,
	)
	if err != nil {
		return fmt.Errorf("marking incoming query as finished: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("incoming query not found: %d", id)
	}

	return nil
}

// CancelIncomingQuery marks an incoming query as canceled with an error message
func (s *storageSQLite) CancelIncomingQuery(id IncomingQueryID, errorMsg string, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE incoming_queries SET state = ?, finished_at = ?, error = ?, rows_read = ?, bytes_read = ? WHERE id = ?",
		string(QueryStateCancelled), finishedAt, errorMsg, stats.Rows, stats.Bytes, id,
	)
	if err != nil {
		return fmt.Errorf("canceling incoming query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("incoming query not found: %d", id)
	}

	return nil
}

// ListIncomingQueries retrieves a list of incoming queries with optional filtering
func (s *storageSQLite) ListIncomingQueries(state *QueryState, limit, offset int) ([]*IncomingQuery, error) {
	var querySQL string
	var args []any

	if state == nil {
		querySQL = `
			SELECT id, data_source_kind, rows_read, bytes_read, state, created_at, finished_at, error
			FROM incoming_queries ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{limit, offset}
	} else {
		querySQL = `
			SELECT id, data_source_kind, rows_read, bytes_read, state, created_at, finished_at, error
			FROM incoming_queries WHERE state = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{string(*state), limit, offset}
	}

	rows, err := s.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("listing incoming queries: %w", err)
	}
	defer rows.Close()

	var queries []*IncomingQuery
	for rows.Next() {
		var query IncomingQuery
		var finishedAt sql.NullTime
		var errorMsg sql.NullString

		if err := rows.Scan(
			&query.ID, &query.DataSourceKind, &query.RowsRead, &query.BytesRead,
			&query.State, &query.CreatedAt, &finishedAt, &errorMsg,
		); err != nil {
			return nil, fmt.Errorf("scanning incoming query: %w", err)
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
		return nil, fmt.Errorf("iterating incoming queries: %w", err)
	}

	return queries, nil
}

// CreateOutgoingQuery creates a new outgoing query associated with an incoming query
func (s *storageSQLite) CreateOutgoingQuery(
	logger *zap.Logger,
	incomingQueryID IncomingQueryID,
	dsi *api_common.TGenericDataSourceInstance,
	queryText string,
	queryArgs []any,
) (OutgoingQueryID, error) {
	// Start a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("starting transaction: %w", err)
	}

	// Define a function to rollback if needed
	rollback := func() {
		if rbErr := tx.Rollback(); rbErr != nil {
			logger.Error("tx rollback", zap.Error(err))
		}
	}

	// First check if the incoming query exists
	var exists bool
	err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM incoming_queries WHERE id = ?)", incomingQueryID).Scan(&exists)
	if err != nil {
		rollback()
		return 0, fmt.Errorf("checking incoming query existence: %w", err)
	}

	if !exists {
		rollback()
		return 0, fmt.Errorf("incoming query not found: %d", incomingQueryID)
	}

	query := &OutgoingQuery{
		IncomingQueryID:  incomingQueryID,
		DatabaseName:     dsi.Database,
		DatabaseEndpoint: common.EndpointToString(dsi.Endpoint),
		QueryText:        queryText,
		QueryArgs:        fmt.Sprint(queryArgs),
		CreatedAt:        time.Now().UTC(),
		State:            QueryStateRunning,
	}

	// Execute the insert within the transaction
	result, err := tx.Exec(
		`INSERT INTO outgoing_queries 
		(incoming_query_id, database_name, database_endpoint, query_text, query_args, created_at, state) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		query.IncomingQueryID, query.DatabaseName, query.DatabaseEndpoint,
		query.QueryText, query.QueryArgs, query.CreatedAt, string(query.State),
	)
	if err != nil {
		rollback()
		return 0, fmt.Errorf("creating outgoing query: %w", err)
	}

	// Get the auto-generated ID
	id, err := result.LastInsertId()
	if err != nil {
		rollback()
		return 0, fmt.Errorf("retrieving generated ID: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		rollback()
		return 0, fmt.Errorf("committing transaction: %w", err)
	}

	return OutgoingQueryID(id), nil
}

// FinishOutgoingQuery marks an outgoing query as finished
func (s *storageSQLite) FinishOutgoingQuery(id OutgoingQueryID) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE outgoing_queries SET state = ?, finished_at = ? WHERE id = ?",
		string(QueryStateFinished), finishedAt, id,
	)
	if err != nil {
		return fmt.Errorf("marking outgoing query as finished: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("outgoing query not found: %d", id)
	}

	return nil
}

// CancelOutgoingQuery marks an outgoing query as canceled with an error message
func (s *storageSQLite) CancelOutgoingQuery(id OutgoingQueryID, errorMsg string) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE outgoing_queries SET state = ?, finished_at = ?, error = ? WHERE id = ?",
		string(QueryStateCancelled), finishedAt, errorMsg, id,
	)
	if err != nil {
		return fmt.Errorf("canceling outgoing query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("outgoing query not found: %d", id)
	}

	return nil
}

// ListOutgoingQueries retrieves a list of outgoing queries with optional filtering
func (s *storageSQLite) ListOutgoingQueries(incomingQueryID *IncomingQueryID, state *QueryState, limit, offset int) ([]*OutgoingQuery, error) {
	var querySQL string
	var args []any

	// Build the query based on which filters are provided
	if incomingQueryID != nil && state != nil {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error
			FROM outgoing_queries 
			WHERE incoming_query_id = ? AND state = ? 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{*incomingQueryID, string(*state), limit, offset}
	} else if incomingQueryID != nil {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error
			FROM outgoing_queries 
			WHERE incoming_query_id = ? 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{*incomingQueryID, limit, offset}
	} else if state != nil {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error
			FROM outgoing_queries 
			WHERE state = ? 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{string(*state), limit, offset}
	} else {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error
			FROM outgoing_queries 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{limit, offset}
	}

	rows, err := s.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("listing outgoing queries: %w", err)
	}
	defer rows.Close()

	var queries []*OutgoingQuery
	for rows.Next() {
		var query OutgoingQuery
		var finishedAt sql.NullTime
		var queryText, queryArgs, errorMsg sql.NullString

		if err := rows.Scan(
			&query.ID, &query.IncomingQueryID, &query.DatabaseName, &query.DatabaseEndpoint,
			&queryText, &queryArgs, &query.State, &query.CreatedAt, &finishedAt, &errorMsg,
		); err != nil {
			return nil, fmt.Errorf("scanning outgoing query: %w", err)
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
		return nil, fmt.Errorf("iterating outgoing queries: %w", err)
	}

	return queries, nil
}

// ListSimilarIncomingQueriesWithDifferentStats finds incoming queries with same outgoing query text but different resource usage
func (s *storageSQLite) ListSimilarIncomingQueriesWithDifferentStats() ([][]*IncomingQuery, error) {
	// Step 1: Find outgoing queries with the same text and args that are associated with different incoming queries
	findSimilarSQL := `
	WITH query_groups AS (
		SELECT 
			query_text, 
			query_args,
			COUNT(DISTINCT incoming_query_id) as distinct_incoming_queries
		FROM 
			outgoing_queries
		WHERE 
			query_text IS NOT NULL AND
			query_text != '' AND
			state != ?
		GROUP BY 
			query_text, query_args
		HAVING 
			distinct_incoming_queries > 1
	)
	SELECT 
		query_text, query_args
	FROM 
		query_groups
	LIMIT 100;
	`

	rows, err := s.db.Query(findSimilarSQL, string(QueryStateRunning))
	if err != nil {
		return nil, fmt.Errorf("finding similar outgoing queries: %w", err)
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

	// Step 2: For each unique query text+args, find all incoming queries that have these outgoing queries
	var result [][]*IncomingQuery
	for _, key := range keys {
		// Get the incoming query IDs that have outgoing queries with this text+args
		incomingIDsSQL := `
		WITH incoming_ids AS (
			SELECT DISTINCT incoming_query_id
			FROM outgoing_queries
			WHERE query_text = ? AND query_args = ? AND state != ?
		)
		SELECT 
			iq.id, iq.data_source_kind, iq.rows_read, iq.bytes_read, 
			iq.state, iq.created_at, iq.finished_at, iq.error
		FROM 
			incoming_queries iq
		JOIN 
			incoming_ids i ON iq.id = i.incoming_query_id
		WHERE
			iq.state != ?
		ORDER BY 
			iq.created_at DESC
		`

		qrows, err := s.db.Query(incomingIDsSQL, key.text, key.args,
			string(QueryStateRunning), string(QueryStateRunning))
		if err != nil {
			return nil, fmt.Errorf("fetching incoming query group: %w", err)
		}

		var group []*IncomingQuery
		for qrows.Next() {
			var query IncomingQuery
			var finishedAt sql.NullTime
			var errorMsg sql.NullString

			if err := qrows.Scan(
				&query.ID, &query.DataSourceKind, &query.RowsRead, &query.BytesRead,
				&query.State, &query.CreatedAt, &finishedAt, &errorMsg,
			); err != nil {
				qrows.Close()
				return nil, fmt.Errorf("scanning incoming query: %w", err)
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
			return nil, fmt.Errorf("iterating incoming query group: %w", err)
		}

		// Step 3: Check if these incoming queries have different resource usage
		if len(group) > 1 {
			// Check if there are variations in resource usage
			hasDifferentStats := false
			var firstRowsRead, firstBytesRead int64
			if len(group) > 0 {
				firstRowsRead = group[0].RowsRead
				firstBytesRead = group[0].BytesRead
			}

			for i := 1; i < len(group); i++ {
				if group[i].RowsRead != firstRowsRead || group[i].BytesRead != firstBytesRead {
					hasDifferentStats = true
					break
				}
			}

			if hasDifferentStats {
				result = append(result, group)
			}
		}
	}

	return result, nil
}

// Close closes the database connection
func (s *storageSQLite) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// newStorageSQLite creates a new Storage instance
func newStorageSQLite(cfg *config.TObservationConfig_TStorage_TSQLite) (Storage, error) {
	db, err := sql.Open("sqlite3", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite database: %w", err)
	}

	db.SetMaxOpenConns(1) // Limit to 1 connection for SQLite
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	// Set pragmas for better performance
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA cache_size=5000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=30000000000",
		"PRAGMA busy_timeout=5000",
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
