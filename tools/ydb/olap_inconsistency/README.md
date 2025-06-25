# YDB OLAP Inconsistency Detector

This tool helps detect inconsistencies in YDB OLAP query results. It runs a specified YQL query against multiple tablet IDs in parallel and monitors for inconsistencies in the number of rows returned between consecutive runs.

## Problem Description

Sometimes a YQL query returns inconsistent data (for example, 10000 lines in the first launch and only 10 lines in the second launch, and then 10000 lines in the following queries), even though the database state hasn't changed. This tool helps identify which tablet ID is causing the inconsistency.

## How It Works

1. The application runs the provided YQL query template for each possible TabletId in separate goroutines.
2. Each goroutine runs the query periodically (default: every 5 seconds).
3. If an inconsistency is detected (different number of rows returned between runs for the same TabletId), the application stops and prints the problematic TabletId.

## Usage

```bash
go run main.go -endpoint=<endpoint> -database=<database> -token=<token> [-interval=<interval>]
```

### Parameters

- `endpoint`: YDB endpoint (default: "localhost:2136")
- `database`: YDB database path (default: "/local")
- `token`: IAM token for authentication (required)
- `interval`: Query interval in seconds (default: 5)

## Example

```bash
go run main.go -endpoint=ydb.example.com:2136 -database=/local -token=t1.9euelZrMzMzNzM3M -interval=10
```

This will run the query every 10 seconds for each tablet ID and stop when an inconsistency is detected.

## Output

The application logs the row count for each tablet ID on each run. If an inconsistency is detected, it logs the tablet ID and the different row counts, then exits.

Example output:
```
2025-06-25T14:52:12.123+0300	INFO	connecting to YDB	{"dsn": "grpc://ydb.example.com:2136/local", "auth": "IAM token"}
2025-06-25T14:52:12.234+0300	INFO	starting monitoring for tablet ID	{"tablet_id": "72075186235526786"}
2025-06-25T14:52:12.234+0300	INFO	starting monitoring for tablet ID	{"tablet_id": "72075186235526433"}
...
2025-06-25T14:52:17.345+0300	INFO	query executed	{"tablet_id": "72075186235526786", "query_num": 1, "row_count": 10000}
2025-06-25T14:52:17.345+0300	INFO	query executed	{"tablet_id": "72075186235526433", "query_num": 1, "row_count": 5000}
...
2025-06-25T14:52:27.456+0300	INFO	query executed	{"tablet_id": "72075186235526786", "query_num": 3, "row_count": 10}
2025-06-25T14:52:27.456+0300	INFO	inconsistency detected	{"tablet_id": "72075186235526786", "query_num": 3, "previous_count": 10000, "current_count": 10}
2025-06-25T14:52:27.456+0300	INFO	inconsistency found in tablet ID	{"tablet_id": "72075186235526786"}
2025-06-25T14:52:27.456+0300	INFO	context canceled, waiting for goroutines to finish...
2025-06-25T14:52:27.456+0300	INFO	context canceled for tablet ID	{"tablet_id": "72075186235526433", "total_queries": 3}
...
2025-06-25T14:52:27.567+0300	INFO	all goroutines finished, exiting...