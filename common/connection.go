package common

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

func makeConnection(logger *zap.Logger, cfg *config.TClientConfig, additionalOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if cfg.Tls != nil {
		logger.Info("client will use TLS connections")

		caCrt, err := os.ReadFile(cfg.Tls.Ca)
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCrt) {
			return nil, fmt.Errorf("failed to add server CA's certificate")
		}

		tlsCfg := &tls.Config{
			RootCAs: certPool,
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		logger.Info("client will use insecure connections")

		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	opts = append(opts, additionalOpts...)

	conn, err := grpc.Dial(EndpointToString(cfg.Endpoint), opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return conn, nil
}
