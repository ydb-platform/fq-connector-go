package observation

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	observation "github.com/ydb-platform/fq-connector-go/api/observation"
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
		rows_read INTEGER NOT NULL DEFAULT 0,
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

// Helper function to convert state enum to string
func stateToString(state observation.QueryState) string {
	switch state {
	case observation.QueryState_QUERY_STATE_RUNNING:
		return "running"
	case observation.QueryState_QUERY_STATE_FINISHED:
		return "finished"
	case observation.QueryState_QUERY_STATE_CANCELLED:
		return "canceled"
	default:
		return "unknown"
	}
}

// Helper function to convert string to state enum
func stringToState(state string) observation.QueryState {
	switch state {
	case "running":
		return observation.QueryState_QUERY_STATE_RUNNING
	case "finished":
		return observation.QueryState_QUERY_STATE_FINISHED
	case "canceled":
		return observation.QueryState_QUERY_STATE_CANCELLED
	default:
		return observation.QueryState_QUERY_STATE_UNSPECIFIED
	}
}

// CreateIncomingQuery creates a new incoming query record
func (s *storageSQLite) CreateIncomingQuery(dataSourceKind api_common.EGenericDataSourceKind) (uint64, error) {
	now := time.Now().UTC()

	result, err := s.db.Exec(
		"INSERT INTO incoming_queries (data_source_kind, created_at, rows_read, bytes_read, state) VALUES (?, ?, ?, ?, ?)",
		dataSourceKind.String(), now, 0, 0, stateToString(observation.QueryState_QUERY_STATE_RUNNING),
	)
	if err != nil {
		return 0, fmt.Errorf("creating incoming query: %w", err)
	}

	// Get the auto-generated ID
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("retrieving generated ID: %w", err)
	}

	return uint64(id), nil
}

// FinishIncomingQuery marks an incoming query as finished with final stats
func (s *storageSQLite) FinishIncomingQuery(id uint64, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE incoming_queries SET state = ?, finished_at = ?, rows_read = ?, bytes_read = ? WHERE id = ?",
		stateToString(observation.QueryState_QUERY_STATE_FINISHED), finishedAt, stats.Rows, stats.Bytes, id,
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
func (s *storageSQLite) CancelIncomingQuery(
	id uint64,
	errorMsg string,
	stats *api_service_protos.TReadSplitsResponse_TStats,
) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE incoming_queries SET state = ?, finished_at = ?, error = ?, rows_read = ?, bytes_read = ? WHERE id = ?",
		stateToString(observation.QueryState_QUERY_STATE_CANCELLED), finishedAt, errorMsg, stats.Rows, stats.Bytes, id,
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
func (s *storageSQLite) ListIncomingQueries(state *observation.QueryState, limit, offset int) ([]*observation.IncomingQuery, error) {
	var (
		querySQL string
		args     []any
	)

	if state == nil || *state == observation.QueryState_QUERY_STATE_UNSPECIFIED {
		querySQL = `
			SELECT id, data_source_kind, rows_read, bytes_read, state, created_at, finished_at, error
			FROM incoming_queries ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{limit, offset}
	} else {
		querySQL = `
			SELECT id, data_source_kind, rows_read, bytes_read, state, created_at, finished_at, error
			FROM incoming_queries WHERE state = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{stateToString(*state), limit, offset}
	}

	rows, err := s.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("listing incoming queries: %w", err)
	}
	defer rows.Close()

	var queries []*observation.IncomingQuery

	for rows.Next() {
		var (
			id             uint64
			dataSourceKind string
			rowsRead       int64
			bytesRead      int64
			stateStr       string
			createdAt      time.Time
			finishedAt     sql.NullTime
			errorMsg       sql.NullString
		)

		if err := rows.Scan(
			&id, &dataSourceKind, &rowsRead, &bytesRead,
			&stateStr, &createdAt, &finishedAt, &errorMsg,
		); err != nil {
			return nil, fmt.Errorf("scanning incoming query: %w", err)
		}

		query := &observation.IncomingQuery{
			Id:             id,
			DataSourceKind: dataSourceKind,
			RowsRead:       rowsRead,
			BytesRead:      bytesRead,
			State:          stringToState(stateStr),
			CreatedAt:      timestamppb.New(createdAt),
		}

		if errorMsg.Valid {
			query.Error = errorMsg.String
		}

		if finishedAt.Valid {
			query.FinishedAt = timestamppb.New(finishedAt.Time)
		}

		queries = append(queries, query)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating incoming queries: %w", err)
	}

	return queries, nil
}

// CreateOutgoingQuery creates a new outgoing query associated with an incoming query
func (s *storageSQLite) CreateOutgoingQuery(
	logger *zap.Logger,
	incomingQueryID uint64,
	dsi *api_common.TGenericDataSourceInstance,
	queryText string,
	queryArgs []any,
) (uint64, error) {
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

	now := time.Now().UTC()

	// Execute the insert within the transaction
	result, err := tx.Exec(
		`INSERT INTO outgoing_queries 
		(incoming_query_id, database_name, database_endpoint, rows_read, query_text, query_args, created_at, state) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		incomingQueryID, dsi.Database, common.EndpointToString(dsi.Endpoint),
		0, queryText, fmt.Sprint(queryArgs), now, stateToString(observation.QueryState_QUERY_STATE_RUNNING),
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

	return uint64(id), nil
}

// FinishOutgoingQuery marks an outgoing query as finished
func (s *storageSQLite) FinishOutgoingQuery(id uint64, rowsRead int64) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE outgoing_queries SET state = ?, finished_at = ?, rows_read = ? WHERE id = ?",
		stateToString(observation.QueryState_QUERY_STATE_FINISHED), finishedAt, rowsRead, id,
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
func (s *storageSQLite) CancelOutgoingQuery(id uint64, errorMsg string) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE outgoing_queries SET state = ?, finished_at = ?, error = ? WHERE id = ?",
		stateToString(observation.QueryState_QUERY_STATE_CANCELLED), finishedAt, errorMsg, id,
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
func (s *storageSQLite) ListOutgoingQueries(
	incomingQueryID *uint64,
	state *observation.QueryState,
	limit, offset int,
) ([]*observation.OutgoingQuery, error) {
	var (
		querySQL string
		args     []any
	)

	stateStr := ""
	if state != nil && *state != observation.QueryState_QUERY_STATE_UNSPECIFIED {
		stateStr = stateToString(*state)
	}

	// Build the query based on which filters are provided
	if incomingQueryID != nil && stateStr != "" {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error, rows_read
			FROM outgoing_queries 
			WHERE incoming_query_id = ? AND state = ? 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{*incomingQueryID, stateStr, limit, offset}
	} else if incomingQueryID != nil {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error, rows_read
			FROM outgoing_queries 
			WHERE incoming_query_id = ? 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{*incomingQueryID, limit, offset}
	} else if stateStr != "" {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error, rows_read
			FROM outgoing_queries 
			WHERE state = ? 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{stateStr, limit, offset}
	} else {
		querySQL = `
			SELECT id, incoming_query_id, database_name, database_endpoint, query_text, 
			       query_args, state, created_at, finished_at, error, rows_read
			FROM outgoing_queries 
			ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []any{limit, offset}
	}

	rows, err := s.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("listing outgoing queries: %w", err)
	}
	defer rows.Close()

	var queries []*observation.OutgoingQuery

	for rows.Next() {
		var (
			id                             uint64
			incomingQueryID                uint64
			databaseName                   string
			databaseEndpoint               string
			stateStr                       string
			createdAt                      time.Time
			rowsRead                       int64
			finishedAt                     sql.NullTime
			queryText, queryArgs, errorMsg sql.NullString
		)

		if err := rows.Scan(
			&id, &incomingQueryID, &databaseName, &databaseEndpoint,
			&queryText, &queryArgs, &stateStr, &createdAt, &finishedAt, &errorMsg, &rowsRead,
		); err != nil {
			return nil, fmt.Errorf("scanning outgoing query: %w", err)
		}

		query := &observation.OutgoingQuery{
			Id:               id,
			IncomingQueryId:  incomingQueryID,
			DatabaseName:     databaseName,
			DatabaseEndpoint: databaseEndpoint,
			State:            stringToState(stateStr),
			CreatedAt:        timestamppb.New(createdAt),
			RowsRead:         rowsRead,
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
			query.FinishedAt = timestamppb.New(finishedAt.Time)
		}

		queries = append(queries, query)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating outgoing queries: %w", err)
	}

	return queries, nil
}

// Close closes the database connection
func (s *storageSQLite) Close() error {
	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

// newStorageSQLite creates a new Storage instance
func newStorageSQLite(logger *zap.Logger, cfg *config.TObservationConfig_TStorage_TSQLite) (Storage, error) {
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
		common.LogCloserError(logger, db, "close SQLite database")
		return nil, fmt.Errorf("initialize: %w", err)
	}

	return storage, nil
}
