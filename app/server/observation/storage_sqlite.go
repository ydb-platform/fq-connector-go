package observation

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/api/observation"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ Storage = (*storageSQLite)(nil)

// storageSQLite handles storing and retrieving query data
type storageSQLite struct {
	db                      *sql.DB
	exitChan                chan struct{}
	createIncomingQueryStmt *sql.Stmt
	finishIncomingQueryStmt *sql.Stmt
	cancelIncomingQueryStmt *sql.Stmt
	logger                  *zap.Logger
}

// initialize creates the necessary tables and prepared statements
func (s *storageSQLite) initialize() error {
	// Create the incoming_queries table
	createIncomingTableSQL := `
	CREATE TABLE IF NOT EXISTS incoming_queries (
		id TEXT PRIMARY KEY,
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
		id TEXT PRIMARY KEY,
		incoming_query_id TEXT NOT NULL,
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

	var err error

	// Enable foreign key support
	_, err = s.db.Exec("PRAGMA foreign_keys = ON;")
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

	// Prepare statements for better performance
	s.createIncomingQueryStmt, err = s.db.Prepare(
		"INSERT INTO incoming_queries (id, data_source_kind, created_at, rows_read, bytes_read, state) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("preparing create incoming query statement: %w", err)
	}

	s.finishIncomingQueryStmt, err = s.db.Prepare(
		"UPDATE incoming_queries SET state = ?, finished_at = ?, rows_read = ?, bytes_read = ? WHERE id = ?")
	if err != nil {
		return fmt.Errorf("preparing finish incoming query statement: %w", err)
	}

	s.cancelIncomingQueryStmt, err = s.db.Prepare(
		"UPDATE incoming_queries SET state = ?, finished_at = ?, error = ?, rows_read = ?, bytes_read = ? WHERE id = ?")
	if err != nil {
		return fmt.Errorf("preparing cancel incoming query statement: %w", err)
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
	case observation.QueryState_QUERY_STATE_CANCELED:
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
		return observation.QueryState_QUERY_STATE_CANCELED
	default:
		return observation.QueryState_QUERY_STATE_UNSPECIFIED
	}
}

// CreateIncomingQuery creates a new incoming query record
func (s *storageSQLite) CreateIncomingQuery(
	ctx context.Context,
	logger *zap.Logger,
	dataSourceKind api_common.EGenericDataSourceKind,
) (*zap.Logger, string, error) {
	now := time.Now().UTC()
	id := uuid.NewString()

	// Use the prepared statement for better performance
	_, err := s.createIncomingQueryStmt.ExecContext(ctx,
		id, dataSourceKind.String(), now, 0, 0, stateToString(observation.QueryState_QUERY_STATE_RUNNING),
	)
	if err != nil {
		return logger, "", fmt.Errorf("creating incoming query: %w", err)
	}

	logger = logger.With(zap.String("incoming_query_id", id))

	logger.Debug("created incoming query")

	return logger, id, nil
}

// FinishIncomingQuery marks an incoming query as finished with final stats
func (s *storageSQLite) FinishIncomingQuery(
	ctx context.Context, logger *zap.Logger, id string, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	finishedAt := time.Now().UTC()

	result, err := s.finishIncomingQueryStmt.ExecContext(ctx,
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
		return fmt.Errorf("incoming query not found: %s", id)
	}

	logger.Debug("finished incoming query")

	return nil
}

// CancelIncomingQuery marks an incoming query as canceled with an error message
func (s *storageSQLite) CancelIncomingQuery(ctx context.Context, logger *zap.Logger,
	id string,
	errorMsg string,
	stats *api_service_protos.TReadSplitsResponse_TStats,
) error {
	finishedAt := time.Now().UTC()

	result, err := s.cancelIncomingQueryStmt.ExecContext(ctx,
		stateToString(observation.QueryState_QUERY_STATE_CANCELED), finishedAt, errorMsg, stats.Rows, stats.Bytes, id,
	)
	if err != nil {
		return fmt.Errorf("canceling incoming query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("incoming query not found: %s", id)
	}

	logger.Debug("canceled incoming query")

	return nil
}

// ListIncomingQueries retrieves a list of incoming queries with optional filtering
func (s *storageSQLite) ListIncomingQueries(
	ctx context.Context, _ *zap.Logger, state *observation.QueryState, limit, offset int,
) ([]*observation.IncomingQuery, error) {
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

	rows, err := s.db.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("listing incoming queries: %w", err)
	}
	defer rows.Close()

	var queries []*observation.IncomingQuery

	for rows.Next() {
		var (
			id             string
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
	ctx context.Context,
	logger *zap.Logger,
	incomingQueryID string,
	dsi *api_common.TGenericDataSourceInstance,
	queryText string,
	queryArgs []any,
) (*zap.Logger, string, error) {
	// Start a transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return logger, "", fmt.Errorf("starting transaction: %w", err)
	}

	// Define a function to rollback if needed
	rollback := func() {
		if rbErr := tx.Rollback(); rbErr != nil {
			logger.Error("tx rollback", zap.Error(err))
		}
	}

	// First check if the incoming query exists
	var exists bool

	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM incoming_queries WHERE id = ?)", incomingQueryID).Scan(&exists)
	if err != nil {
		rollback()
		return logger, "", fmt.Errorf("checking incoming query existence: %w", err)
	}

	if !exists {
		rollback()
		return logger, "", fmt.Errorf("incoming query not found: %s", incomingQueryID)
	}

	now := time.Now().UTC()
	id := uuid.NewString()

	// Execute the insert within the transaction
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outgoing_queries
		(id, incoming_query_id, database_name, database_endpoint, rows_read, query_text, query_args, created_at, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, incomingQueryID, dsi.Database, common.EndpointToString(dsi.Endpoint),
		0, queryText, fmt.Sprint(queryArgs), now, stateToString(observation.QueryState_QUERY_STATE_RUNNING),
	)
	if err != nil {
		rollback()
		return logger, "", fmt.Errorf("creating outgoing query: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		rollback()
		return logger, "", fmt.Errorf("committing transaction: %w", err)
	}

	logger = logger.With(zap.String("outgoing_query_id", id))
	logger.Debug("created outgoing query")

	return logger, id, nil
}

// FinishOutgoingQuery marks an outgoing query as finished
func (s *storageSQLite) FinishOutgoingQuery(_ context.Context, logger *zap.Logger, id string, rowsRead int64) error {
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
		return fmt.Errorf("outgoing query not found: %s", id)
	}

	logger.Debug("finished outgoing query")

	return nil
}

// CancelOutgoingQuery marks an outgoing query as canceled with an error message
func (s *storageSQLite) CancelOutgoingQuery(_ context.Context, logger *zap.Logger, id string, errorMsg string) error {
	finishedAt := time.Now().UTC()

	result, err := s.db.Exec(
		"UPDATE outgoing_queries SET state = ?, finished_at = ?, error = ? WHERE id = ?",
		stateToString(observation.QueryState_QUERY_STATE_CANCELED), finishedAt, errorMsg, id,
	)
	if err != nil {
		return fmt.Errorf("canceling outgoing query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("outgoing query not found: %s", id)
	}

	logger.Debug("canceled outgoing query")

	return nil
}

// ListOutgoingQueries retrieves a list of outgoing queries with optional filtering
func (s *storageSQLite) ListOutgoingQueries(_ context.Context, _ *zap.Logger,
	incomingQueryID *string,
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
		args = []any{fmt.Sprint(*incomingQueryID), limit, offset}
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
			id                             string
			incomingQueryID                string
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
			IncomingQueryId:  incomingQueryID, // Convert string to uint64
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
func (s *storageSQLite) Close(_ context.Context) error {
	if s.db != nil {
		close(s.exitChan) // Signal the garbage collector to stop

		// Close prepared statements
		if err := s.createIncomingQueryStmt.Close(); err != nil {
			s.logger.Error("failed to close create incoming query statement", zap.Error(err))
		}

		if err := s.finishIncomingQueryStmt.Close(); err != nil {
			s.logger.Error("failed to close finish incoming query statement", zap.Error(err))
		}

		if err := s.cancelIncomingQueryStmt.Close(); err != nil {
			s.logger.Error("failed to close cancel incoming query statement", zap.Error(err))
		}

		if err := s.db.Close(); err != nil {
			s.logger.Error("failed to close database", zap.Error(err))
		}
	}

	return nil
}

// newStorageSQLite creates a new Storage instance
func (s *storageSQLite) getDatabaseSize() (int64, error) {
	var size int64

	err := s.db.QueryRow(`SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();`).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to get database size: %w", err)
	}

	return size, nil
}

func (s *storageSQLite) collectGarbage(logger *zap.Logger, ttl time.Duration) {
	cutoff := time.Now().Add(-ttl).UTC()

	// Log storage size before cleanup
	sizeBefore, err := s.getDatabaseSize()
	if err != nil {
		logger.Error("failed to get storage size before cleanup", zap.Error(err))
	}

	_, err = s.db.Exec(`
        DELETE FROM incoming_queries WHERE created_at < ?;
        DELETE FROM outgoing_queries WHERE created_at < ?;
    `, cutoff, cutoff)
	if err != nil {
		logger.Error("failed to clean up old queries", zap.Error(err))
	}

	_, err = s.db.Exec(` VACUUM; `)
	if err != nil {
		logger.Error("failed to vacuum database", zap.Error(err))
	}

	// Log storage size after cleanup
	sizeAfter, err := s.getDatabaseSize()
	if err != nil {
		logger.Error("failed to get storage size after cleanup", zap.Error(err))
	}

	logger.Info("garbage collection completed", zap.Int64("size_before", sizeBefore), zap.Int64("size_after", sizeAfter))
}

func (s *storageSQLite) startGarbageCollector(logger *zap.Logger, ttl time.Duration, gcPeriod time.Duration) {
	go func() {
		ticker := time.NewTicker(gcPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-s.exitChan:
				return
			case <-ticker.C:
				s.collectGarbage(logger, ttl)
			}
		}
	}()
}

// newStorageSQLite creates a new Storage instance
func newStorageSQLite(logger *zap.Logger, cfg *config.TObservationConfig_TStorage_TSQLite) (Storage, error) {
	db, err := sql.Open("sqlite3", cfg.Path+"?_txlock=immediate&_journal=WAL&_sync=OFF&_secure_delete=FALSE&_mutex=no&cache=shared")
	if err != nil {
		return nil, fmt.Errorf("opening SQLite database: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Set pragmas for maximum performance
	pragmas := []string{
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = WAL",
		"PRAGMA locking_mode = NORMAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 30000000000",
		"PRAGMA page_size = 4096",
		"PRAGMA cache_size = 10000",
		"PRAGMA auto_vacuum = INCREMENTAL",
	}

	for _, pragma := range pragmas {
		if _, err = db.Exec(pragma); err != nil {
			return nil, fmt.Errorf("setting pragma %s: %w", pragma, err)
		}
	}

	// Initialize storage
	storage := &storageSQLite{
		db:       db,
		exitChan: make(chan struct{}),
	}

	if err = storage.initialize(); err != nil {
		common.LogCloserError(logger, db, "close SQLite database")
		return nil, fmt.Errorf("initialize: %w", err)
	}

	// Initialize garbage collector
	requestTTL, err := common.DurationFromString(cfg.RequestTtl)
	if err != nil {
		return nil, fmt.Errorf("invalid request TTL: %w", err)
	}

	gcPeriod, err := common.DurationFromString(cfg.GcPeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid GC period: %w", err)
	}

	storage.startGarbageCollector(logger, requestTTL, gcPeriod)

	return storage, nil
}
