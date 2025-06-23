package observation

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_observation "github.com/ydb-platform/fq-connector-go/api/observation"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/observation/discovery"
	"github.com/ydb-platform/fq-connector-go/common"
)

// aggregationServer handles WebSocket connections and polling
type aggregationServer struct {
	discovery       discovery.Discovery
	pollingInterval time.Duration
	upgrader        *websocket.Upgrader
	cfg             *config.TObservationServerConfig
	logger          *zap.Logger
}

// newAggregationServer creates a new aggregation server instance
func newAggregationServer(cfg *config.TObservationServerConfig) (*aggregationServer, error) {
	logger := common.NewDefaultLogger()

	d, err := discovery.NewDiscovery(cfg.Discovery)
	if err != nil {
		return nil, fmt.Errorf("new discovery: %w", err)
	}

	pollingInterval := common.MustDurationFromString(cfg.PollingInterval)

	return &aggregationServer{
		discovery:       d,
		pollingInterval: pollingInterval,
		logger:          logger,
		cfg:             cfg,
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}, nil
}

// Start begins the HTTP server
func (s *aggregationServer) start() error {
	endpoint := common.EndpointToString(s.cfg.Endpoint)

	s.logger.Info("starting aggregation server",
		zap.String("endpoint", endpoint),
		zap.Duration("polling_period", s.pollingInterval),
	)

	http.HandleFunc("/ws", s.handleWebSocket)
	http.Handle("/", getAssetHandler())

	return http.ListenAndServe(endpoint, nil)
}

// handleWebSocket manages WebSocket connections
func (s *aggregationServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	s.logger.Info("new webSocket connection established",
		zap.String("remote_addr", r.RemoteAddr))

	ticker := time.NewTicker(s.pollingInterval)
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
	*api_observation.OutgoingQuery
	FormattedCreatedAt string `json:"formatted_created_at,omitempty"`
}

// pollEndpoints collects queries from all configured endpoints
func (s *aggregationServer) pollEndpoints() map[string][]*QueryWithFormattedTime {
	endpoints, err := s.discovery.GetEndpoints()
	if err != nil {
		s.logger.Error("discover endpoints", zap.Error(err))
		return nil
	}

	results := make(map[string][]*QueryWithFormattedTime)

	s.logger.Info("starting to poll endpoints", zap.Stringers("endpoints", endpoints), zap.Int("count", len(endpoints)))

	for _, endpoint := range endpoints {
		s.logger.Debug("polling endpoint", zap.Stringer("endpoint", endpoint))

		startTime := time.Now()

		queries, err := s.getOutgoingQueries(endpoint)
		duration := time.Since(startTime)

		if err != nil {
			s.logger.Error("error polling endpoint", zap.Stringer("endpoint", endpoint), zap.Error(err))
			continue
		}

		s.logger.Info("endpoint polled successfully",
			zap.Stringer("endpoint", endpoint),
			zap.Duration("duration", duration),
			zap.Int("queries_count", len(queries)))

		if len(queries) > 0 {
			s.logger.Debug("sample query details",
				zap.String("first_query_id", queries[0].Id),
				zap.String("first_query_text", queries[0].QueryText))
		}

		results[common.EndpointToString(endpoint)] = queries
	}

	s.logger.Info("completed polling endpoints",
		zap.Int("successful_endpoints", len(results)),
		zap.Int("failed_endpoints", len(endpoints)-len(results)))

	return results
}

// getOutgoingQueries retrieves running queries from a single endpoint
func (s *aggregationServer) getOutgoingQueries(endpoint *api_common.TGenericEndpoint) ([]*QueryWithFormattedTime, error) {
	s.logger.Debug("connecting to endpoint", zap.Stringer("endpoint", endpoint))

	conn, err := grpc.NewClient(common.EndpointToString(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("new client to %s: %w", endpoint, err)
	}
	defer conn.Close()

	client := api_observation.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &api_observation.ListOutgoingQueriesRequest{
		State: api_observation.QueryState_QUERY_STATE_RUNNING, // Only get RUNNING queries
	}

	s.logger.Debug("making GRPC call to ListOutgoingQueries", zap.Stringer("endpoint", endpoint))

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
			s.logger.Error("stream receive completed",
				zap.Stringer("endpoint", endpoint),
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

	s.logger.Debug(
		"received queries from endpoint",
		zap.Stringer("endpoint", endpoint),
		zap.Int("count", len(queries)),
	)

	return queries, nil
}

// getAssetHandler returns a handler for serving static assets
func getAssetHandler() http.Handler {
	fs := http.Dir("app/observation/assets")
	return http.FileServer(fs)
}

func startAggregationServer(cmd *cobra.Command) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config path: %w", err)
	}

	cfg, err := newConfigFromFile(configPath)
	if err != nil {
		return fmt.Errorf("new config from file: %w", err)
	}

	server, err := newAggregationServer(cfg)
	if err != nil {
		return fmt.Errorf("new aggregation server: %w", err)
	}

	// a blocking call
	if server.start() != nil {
		return fmt.Errorf("start server: %w", err)
	}

	return nil
}
