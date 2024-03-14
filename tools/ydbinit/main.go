package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"

	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func makeRandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func makeConnection(ctx context.Context, endpoint string, database string, useTls bool, token string) (*sql.DB, error) {
	dsn := sugar.DSN(endpoint, database, useTls)

	log.Println("connecting to database", dsn)

	var cred ydb_sdk.Option
	if token != "" {
		cred = ydb_sdk.WithAccessTokenCredentials(token)
	} else {
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

	_, err := conn.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("exec context: %w", err)
	}

	return nil
}

func insertIntoTable(ctx context.Context, conn *sql.DB) error {
	log.Println("inserting into table...")
	const items = 10
	const dataLength = 10

	query := `INSERT INTO large (id, data) VALUES (?, ?)`

	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare query: %w", err)
	}

	defer stmt.Close()

	for i := 0; i < items; i++ {
		if _, err := stmt.ExecContext(ctx, uint64(i), makeRandomString(dataLength)); err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	return nil
}

func selectFromTable(ctx context.Context, conn *sql.DB) error {
	log.Println("selecting from table...")

	query := `SELECT * FROM large`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	defer rows.Close()

	var (
		id   uint64
		data string
	)

	for rows.Next() {
		if err := rows.Scan(&id, &data); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		log.Println(id, data)
	}

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
