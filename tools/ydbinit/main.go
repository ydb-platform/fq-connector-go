package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func makeRandomString(n int, start int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[start%len(letterBytes)]
		start++
	}
	return string(b)
}

func makeConnection(ctx context.Context, endpoint string, database string, useTls bool, token string) (*sql.DB, error) {
	dsn := sugar.DSN(endpoint, database, useTls)

	log.Println("connecting to database", dsn)

	var cred ydb_sdk.Option
	if token != "" {
		log.Println("Using access token credentials")
		cred = ydb_sdk.WithAccessTokenCredentials(token)
	} else {
		log.Println("Using anonymous credentials")
		cred = ydb_sdk.WithAnonymousCredentials()
	}

	ydbDriver, err := ydb_sdk.Open(ctx, dsn, cred)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	ydbConn, err := ydb_sdk.Connector(ydbDriver, ydb_sdk.WithAutoDeclare(), ydb_sdk.WithPositionalArgs())
	if err != nil {
		return nil, fmt.Errorf("connector error: %w", err)
	}

	conn := sql.OpenDB(ydbConn)

	return conn, nil
}

func createTable(ctx context.Context, conn *sql.DB) error {
	log.Println("creating table...")

	query := `
	CREATE TABLE IF NOT EXISTS large (
		id Uint64,
		data String,
		PRIMARY KEY (id)
	)
	`

	_, err := conn.ExecContext(ydb_sdk.WithQueryMode(ctx, ydb_sdk.SchemeQueryMode), query)
	if err != nil {
		return fmt.Errorf("exec context: %w", err)
	}

	return nil
}

const dataLength = 1024

func prepareBulkInsert(start int, rowsPerBatch int) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO large (id, data) VALUES")

	for i := 0; i < rowsPerBatch; i++ {
		sb.WriteString(fmt.Sprintf(" (%d, \"%s\")", start+i, makeRandomString(dataLength, start+i)))
		if i != rowsPerBatch-1 {
			sb.WriteString(",")
		}
	}

	return sb.String()
}

const (
	batches      = 1 << 7
	rowsPerBatch = 1 << 10
)

func insertIntoTable(ctx context.Context, conn *sql.DB) error {
	log.Println("inserting into table...")

	log.Println("expected lines: ", batches*rowsPerBatch)
	log.Println("expected size: ", batches*rowsPerBatch*dataLength)

	for batch := 0; batch < batches; batch++ {
		total := batch * rowsPerBatch
		log.Println("total rows inserted", total)

		query := prepareBulkInsert(total, rowsPerBatch)

		_, err := conn.ExecContext(ctx, query)
		if err != nil {

			// duplicate key is OK
			if ydb_sdk.IsOperationError(err, Ydb.StatusIds_PRECONDITION_FAILED) && strings.Contains(err.Error(), "Conflict with existing key") {
				continue
			}

			return fmt.Errorf("exec: %w", err)
		}
	}

	return nil
}

func selectFromTable(ctx context.Context, conn *sql.DB) error {
	log.Println("selecting from table...")

	query := `SELECT id, data FROM large`

	rows, err := conn.QueryContext(ydb_sdk.WithQueryMode(ctx, ydb_sdk.ScanQueryMode), query)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	defer rows.Close()

	var (
		id   uint64
		data string
	)

	for cont := true; cont; cont = rows.NextResultSet() {

		for rows.Next() {
			if err := rows.Scan(&id, &data); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
		}
	}

	log.Println("LAST scanned key", id, len(data))

	return nil
}

const (
	endpoint string = "localhost:2136"
	database        = "local"
	useTls          = false
)

func run() error {
	token := os.Getenv("YDB_TOKEN")

	ctx := context.Background()

	conn, err := makeConnection(ctx, endpoint, database, useTls, token)
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer conn.Close()

	if err := createTable(ctx, conn); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	if err := insertIntoTable(ctx, conn); err != nil {
		return fmt.Errorf("insert into table: %w", err)
	}

	if err := selectFromTable(ctx, conn); err != nil {
		return fmt.Errorf("select from table: %w", err)
	}

	return nil
}

func main() {
	log.Println(run())
}
