package observation

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/fq-connector-go/api/observation"
	"github.com/ydb-platform/fq-connector-go/common"
)

// NewAggregationServer creates a new aggregation server instance
func NewAggregationServer(endpoints []string, period time.Duration) *AggregationServer {
	logger := common.NewDefaultLogger()

	return &AggregationServer{
		endpoints: endpoints,
		period:    period,
		logger:    logger,
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

// AggregationServer handles WebSocket connections and polling
type AggregationServer struct {
	endpoints []string
	period    time.Duration
	logger    *zap.Logger
	upgrader  *websocket.Upgrader
}

// Start begins the HTTP server
func (s *AggregationServer) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)

	s.logger.Info("starting aggregation server",
		zap.String("address", addr),
		zap.Duration("polling_period", s.period),
		zap.Strings("endpoints", s.endpoints))

	http.HandleFunc("/ws", s.handleWebSocket)
	http.Handle("/", getAssetHandler())

	return http.ListenAndServe(addr, nil)
}

// handleWebSocket manages WebSocket connections
func (s *AggregationServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	s.logger.Info("new webSocket connection established",
		zap.String("remote_addr", r.RemoteAddr))

	ticker := time.NewTicker(s.period)
	defer ticker.Stop()

	for range ticker.C {
		startTime := time.Now()
		queries := s.pollEndpoints()
		duration := time.Since(startTime)

		totalQueries := 0
		for _, q := range queries {
			totalQueries += len(q)
		}

		s.logger.Info("polling completed",
			zap.Duration("duration", duration),
			zap.Int("total_queries", totalQueries),
			zap.Int("endpoints_polled", len(queries)))

		if err := conn.WriteJSON(queries); err != nil {
			s.logger.Error("websocket write failed", zap.Error(err))
			return
		}
	}
}

// QueryWithFormattedTime extends OutgoingQuery with formatted timestamp
type QueryWithFormattedTime struct {
	*observation.OutgoingQuery
	FormattedCreatedAt string `json:"formatted_created_at,omitempty"`
}

// pollEndpoints collects queries from all configured endpoints
func (s *AggregationServer) pollEndpoints() map[string][]*QueryWithFormattedTime {
	results := make(map[string][]*QueryWithFormattedTime)

	s.logger.Info("starting to poll endpoints",
		zap.Strings("endpoints", s.endpoints),
		zap.Int("count", len(s.endpoints)))

	for _, endpoint := range s.endpoints {
		s.logger.Debug("polling endpoint", zap.String("endpoint", endpoint))

		startTime := time.Now()

		queries, err := s.getOutgoingQueries(endpoint)
		duration := time.Since(startTime)

		if err != nil {
			s.logger.Error("error polling endpoint", zap.String("endpoint", endpoint), zap.Error(err))
			continue
		}

		s.logger.Info("endpoint polled successfully",
			zap.String("endpoint", endpoint),
			zap.Duration("duration", duration),
			zap.Int("queries_count", len(queries)))

		if len(queries) > 0 {
			s.logger.Debug("sample query details",
				zap.String("first_query_id", queries[0].Id),
				zap.String("first_query_text", queries[0].QueryText))
		}

		results[endpoint] = queries
	}

	s.logger.Info("completed polling endpoints",
		zap.Int("successful_endpoints", len(results)),
		zap.Int("failed_endpoints", len(s.endpoints)-len(results)))

	return results
}

// getOutgoingQueries retrieves running queries from a single endpoint
func (s *AggregationServer) getOutgoingQueries(endpoint string) ([]*QueryWithFormattedTime, error) {
	s.logger.Debug("connecting to endpoint", zap.String("endpoint", endpoint))

	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", endpoint, err)
	}
	defer conn.Close()

	client := observation.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &observation.ListOutgoingQueriesRequest{
		State: observation.QueryState_QUERY_STATE_RUNNING, // Only get RUNNING queries
	}

	s.logger.Debug("making GRPC call to ListOutgoingQueries",
		zap.String("endpoint", endpoint))

	stream, err := client.ListOutgoingQueries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list outgoing queries: %w", err)
	}

	var (
		queries       []*QueryWithFormattedTime
		receivedCount int
	)

	for {
		resp, err := stream.Recv()
		if err != nil {
			s.logger.Debug("stream receive completed",
				zap.String("endpoint", endpoint),
				zap.Int("total_queries", receivedCount),
				zap.Error(err))

			break
		}

		if resp.Query != nil {
			receivedCount++
			query := &QueryWithFormattedTime{
				OutgoingQuery: resp.Query,
			}

			if resp.Query.CreatedAt != nil {
				query.FormattedCreatedAt = time.Unix(
					resp.Query.CreatedAt.Seconds,
					int64(resp.Query.CreatedAt.Nanos),
				).Format(time.RFC3339)
			}

			queries = append(queries, query)
		}
	}

	s.logger.Debug("received queries from endpoint",
		zap.String("endpoint", endpoint),
		zap.Int("count", len(queries)))

	return queries, nil
}

// getAssetHandler returns a handler for serving static assets
func getAssetHandler() http.Handler {
	fs := http.Dir("app/client/observation/assets")
	return http.FileServer(fs)
}
