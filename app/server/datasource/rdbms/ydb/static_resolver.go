package ydb

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

var _ resolver.Builder = &staticResolverBuilder{}

type clientConn struct {
	resolver.ClientConn
	hostname string
	endpoint string
	logger   *zap.Logger
}

func (c *clientConn) Endpoint() string {
	return c.endpoint
}

func (c *clientConn) UpdateState(state resolver.State) error {
	for _, ep := range state.Endpoints {
		for _, addr := range ep.Addresses {
			if addr.Addr == c.hostname {
				c.logger.Warn("client conn update state: endpoint override", zap.String("before", addr.Addr), zap.String("after", c.endpoint))
				addr.Addr = c.endpoint
			}
		}
	}

	return c.ClientConn.UpdateState(state)
}

type staticResolverBuilder struct {
	resolver.Builder
	scheme   string
	hostname string
	endpoint string
	logger   *zap.Logger
}

func (b *staticResolverBuilder) Build(
	target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if target.Endpoint() == b.hostname {
		b.logger.Warn("static resolver build: endpoint override", zap.String("before", target.Endpoint()), zap.String("after", b.endpoint))

		err := cc.UpdateState(resolver.State{
			Endpoints: []resolver.Endpoint{
				{Addresses: []resolver.Address{{Addr: b.endpoint}}},
			},
		})

		if err != nil {
			return nil, fmt.Errorf("update state: %w", err)
		}
	}

	return b.Builder.Build(target, &clientConn{
		ClientConn: cc,
		hostname:   b.hostname,
		endpoint:   b.endpoint,
		logger:     b.logger,
	}, opts)
}

func (b *staticResolverBuilder) Scheme() string {
	return b.scheme
}

func newStaticResolverBuilder(logger *zap.Logger, scheme, ydbEndpointOverrideRule string) (resolver.Builder, error) {
	// syntax: hostname -> ip:port
	sstr := strings.Split(ydbEndpointOverrideRule, "->")
	if len(sstr) != 2 {
		return nil, fmt.Errorf("bad rule format: '%s'", ydbEndpointOverrideRule)
	}

	rb := &staticResolverBuilder{
		Builder:  resolver.Get("dns"),
		scheme:   scheme,
		hostname: strings.TrimSpace(sstr[0]),
		endpoint: strings.TrimSpace(sstr[1]),
		logger:   logger,
	}
	resolver.Register(rb)

	return rb, nil
}
