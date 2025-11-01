package common //nolint:revive

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

func makeConnection(logger *zap.Logger, cfg *config.TClientConfig, additionalOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(32 * 1024 * 1024)),
	}

	if cfg.Tls != nil {
		tlsCfg := &tls.Config{}

		logger.Info("client will use TLS connections")

		if cfg.Tls.InsecureSkipVerify {
			logger.Warn("Certificate host name verification is disabled")

			tlsCfg.InsecureSkipVerify = true
		}

		// Make custom cert pool only if necessary
		if cfg.Tls.Ca != "" {
			caCrt, err := os.ReadFile(cfg.Tls.Ca)
			if err != nil {
				return nil, fmt.Errorf("read file '%s': %w", cfg.Tls.Ca, err)
			}

			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caCrt) {
				return nil, errors.New("failed to add server CA's certificate")
			}

			tlsCfg.RootCAs = certPool
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		logger.Warn("client will use insecure connections")

		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	opts = append(opts, additionalOpts...)

	conn, err := grpc.NewClient(EndpointToString(cfg.ConnectorServerEndpoint), opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return conn, nil
}
