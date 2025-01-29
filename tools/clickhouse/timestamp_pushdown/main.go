package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	// Define ClickHouse connection info
	connStr := "clickhouse://admin:password@localhost:9000"
	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}
	defer db.Close()

	// Check the connection
	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	// Create context
	ctx := context.Background()

	// 1. Drop previously created table
	dropTableQuery := `DROP TABLE IF EXISTS example_table;`

	_, err = db.ExecContext(ctx, dropTableQuery)
	if err != nil {
		log.Fatalf("failed to drop table: %v", err)
	}
	fmt.Println("Table dropped successfully.")

	// 2. Create a table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS example_table (
			id UInt32,
			datetimeValue DateTime64(8, 'UTC')
		) ENGINE = MergeTree()
		PRIMARY KEY id;`

	_, err = db.ExecContext(ctx, createTableQuery)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	fmt.Println("Table created successfully.")

	// 3. Insert some data into the table
	insertQuery := `
		INSERT INTO example_table (*) VALUES
		(1, '1988-11-20 12:55:28.123456000')
	`

	_, err = db.ExecContext(ctx, insertQuery)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}
	fmt.Println("Data inserted successfully.")

	// 4. Query the table with a filtering expression
	timeValues := []time.Time{
		time.Date(1988, 11, 20, 12, 55, 28, 123456000, time.UTC).Add(time.Nanosecond * 876543999),
		time.Date(1988, 11, 20, 12, 55, 28, 123456000, time.UTC).Add(time.Nanosecond * 876544000),
	}

	for _, timeValue := range timeValues {
		fmt.Printf("\n Checking time value: %v\n", timeValue)

		rows, err := db.QueryContext(ctx, "SELECT id, datetimeValue FROM example_table WHERE datetimeValue > ?", timeValue)
		if err != nil {
			log.Fatalf("failed to execute query: %v", err)
		}
		defer rows.Close()

		fmt.Println("Rows filtered and fetched after query with a filter `datetimeValue > ?`:")
		for rows.Next() {
			var id uint32
			var datetimeValue time.Time
			if err := rows.Scan(&id, &datetimeValue); err != nil {
				log.Fatalf("failed to scan row: %v", err)
			}
			fmt.Printf("ID: %d, DateTime: %s\n", id, datetimeValue.Format(time.RFC3339Nano))
		}

		rows, err = db.QueryContext(ctx, "SELECT id, datetimeValue FROM example_table WHERE datetimeValue < ?", timeValue)
		if err != nil {
			log.Fatalf("failed to execute query: %v", err)
		}
		defer rows.Close()

		fmt.Println("Rows filtered and fetched after query with a filter `datetimeValue < ?`:")
		for rows.Next() {
			var id uint32
			var datetimeValue time.Time
			if err := rows.Scan(&id, &datetimeValue); err != nil {
				log.Fatalf("failed to scan row: %v", err)
			}
			fmt.Printf("ID: %d, DateTime: %s\n", id, datetimeValue.Format(time.RFC3339Nano))
		}

		if err := rows.Err(); err != nil {
			log.Fatalf("rows iteration error: %v", err)
		}
	}
}
