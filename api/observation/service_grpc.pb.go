// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.12.4
// source: api/observation/service.proto

package observation

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ObservationService_ListIncomingQueries_FullMethodName = "/NYql.Connector.Observation.ObservationService/ListIncomingQueries"
	ObservationService_ListOutgoingQueries_FullMethodName = "/NYql.Connector.Observation.ObservationService/ListOutgoingQueries"
)

// ObservationServiceClient is the client API for ObservationService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ObservationServiceClient interface {
	// ListIncomingQueries retrieves a stream of incoming queries based on filter criteria
	ListIncomingQueries(ctx context.Context, in *ListIncomingQueriesRequest, opts ...grpc.CallOption) (ObservationService_ListIncomingQueriesClient, error)
	// ListOutgoingQueries retrieves a stream of outgoing queries based on filter criteria
	ListOutgoingQueries(ctx context.Context, in *ListOutgoingQueriesRequest, opts ...grpc.CallOption) (ObservationService_ListOutgoingQueriesClient, error)
}

type observationServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewObservationServiceClient(cc grpc.ClientConnInterface) ObservationServiceClient {
	return &observationServiceClient{cc}
}

func (c *observationServiceClient) ListIncomingQueries(ctx context.Context, in *ListIncomingQueriesRequest, opts ...grpc.CallOption) (ObservationService_ListIncomingQueriesClient, error) {
	stream, err := c.cc.NewStream(ctx, &ObservationService_ServiceDesc.Streams[0], ObservationService_ListIncomingQueries_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &observationServiceListIncomingQueriesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ObservationService_ListIncomingQueriesClient interface {
	Recv() (*ListIncomingQueriesResponse, error)
	grpc.ClientStream
}

type observationServiceListIncomingQueriesClient struct {
	grpc.ClientStream
}

func (x *observationServiceListIncomingQueriesClient) Recv() (*ListIncomingQueriesResponse, error) {
	m := new(ListIncomingQueriesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *observationServiceClient) ListOutgoingQueries(ctx context.Context, in *ListOutgoingQueriesRequest, opts ...grpc.CallOption) (ObservationService_ListOutgoingQueriesClient, error) {
	stream, err := c.cc.NewStream(ctx, &ObservationService_ServiceDesc.Streams[1], ObservationService_ListOutgoingQueries_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &observationServiceListOutgoingQueriesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ObservationService_ListOutgoingQueriesClient interface {
	Recv() (*ListOutgoingQueriesResponse, error)
	grpc.ClientStream
}

type observationServiceListOutgoingQueriesClient struct {
	grpc.ClientStream
}

func (x *observationServiceListOutgoingQueriesClient) Recv() (*ListOutgoingQueriesResponse, error) {
	m := new(ListOutgoingQueriesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ObservationServiceServer is the server API for ObservationService service.
// All implementations must embed UnimplementedObservationServiceServer
// for forward compatibility
type ObservationServiceServer interface {
	// ListIncomingQueries retrieves a stream of incoming queries based on filter criteria
	ListIncomingQueries(*ListIncomingQueriesRequest, ObservationService_ListIncomingQueriesServer) error
	// ListOutgoingQueries retrieves a stream of outgoing queries based on filter criteria
	ListOutgoingQueries(*ListOutgoingQueriesRequest, ObservationService_ListOutgoingQueriesServer) error
	mustEmbedUnimplementedObservationServiceServer()
}

// UnimplementedObservationServiceServer must be embedded to have forward compatible implementations.
type UnimplementedObservationServiceServer struct {
}

func (UnimplementedObservationServiceServer) ListIncomingQueries(*ListIncomingQueriesRequest, ObservationService_ListIncomingQueriesServer) error {
	return status.Errorf(codes.Unimplemented, "method ListIncomingQueries not implemented")
}
func (UnimplementedObservationServiceServer) ListOutgoingQueries(*ListOutgoingQueriesRequest, ObservationService_ListOutgoingQueriesServer) error {
	return status.Errorf(codes.Unimplemented, "method ListOutgoingQueries not implemented")
}
func (UnimplementedObservationServiceServer) mustEmbedUnimplementedObservationServiceServer() {}

// UnsafeObservationServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ObservationServiceServer will
// result in compilation errors.
type UnsafeObservationServiceServer interface {
	mustEmbedUnimplementedObservationServiceServer()
}

func RegisterObservationServiceServer(s grpc.ServiceRegistrar, srv ObservationServiceServer) {
	s.RegisterService(&ObservationService_ServiceDesc, srv)
}

func _ObservationService_ListIncomingQueries_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListIncomingQueriesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ObservationServiceServer).ListIncomingQueries(m, &observationServiceListIncomingQueriesServer{stream})
}

type ObservationService_ListIncomingQueriesServer interface {
	Send(*ListIncomingQueriesResponse) error
	grpc.ServerStream
}

type observationServiceListIncomingQueriesServer struct {
	grpc.ServerStream
}

func (x *observationServiceListIncomingQueriesServer) Send(m *ListIncomingQueriesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _ObservationService_ListOutgoingQueries_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListOutgoingQueriesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ObservationServiceServer).ListOutgoingQueries(m, &observationServiceListOutgoingQueriesServer{stream})
}

type ObservationService_ListOutgoingQueriesServer interface {
	Send(*ListOutgoingQueriesResponse) error
	grpc.ServerStream
}

type observationServiceListOutgoingQueriesServer struct {
	grpc.ServerStream
}

func (x *observationServiceListOutgoingQueriesServer) Send(m *ListOutgoingQueriesResponse) error {
	return x.ServerStream.SendMsg(m)
}

// ObservationService_ServiceDesc is the grpc.ServiceDesc for ObservationService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ObservationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "NYql.Connector.Observation.ObservationService",
	HandlerType: (*ObservationServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ListIncomingQueries",
			Handler:       _ObservationService_ListIncomingQueries_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ListOutgoingQueries",
			Handler:       _ObservationService_ListOutgoingQueries_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "api/observation/service.proto",
}
