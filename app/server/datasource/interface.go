package datasource

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

type Factory[T paging.Acceptor] interface {
	Make(
		logger *zap.Logger,
		dataSourceType api_common.EGenericDataSourceKind,
	) (DataSource[T], error)
	Close() error
}

type ListSplitResult struct {
	Slct        *api_service_protos.TSelect // the one that was used for split creation
	Description proto.Message               // some representation of the split
}

// DataSource is an abstraction over external data storage that is available for data and metadata extraction.
// All new data sources must implement this interface.
// The types of data extracted from the data source are parametrized via [T paging.Acceptor] interface.
type DataSource[T paging.Acceptor] interface {
	// DescribeTable returns metadata about a table (or similar entity in non-relational data sources)
	// located within a particular database in a cluster of a certain type.
	DescribeTable(
		ctx context.Context,
		logger *zap.Logger,
		request *api_service_protos.TDescribeTableRequest,
	) (*api_service_protos.TDescribeTableResponse, error)

	// ListSplits analyzes the external table and returns the stream of its splits.
	ListSplits(
		ctx context.Context,
		logger *zap.Logger,
		request *api_service_protos.TListSplitsRequest, // contains common settings
		slct *api_service_protos.TSelect, // describes tables that should be read in splits
		resultChan chan<- *ListSplitResult,
	) error

	// ReadSplit is a main method for reading data from the table.
	ReadSplit(
		ctx context.Context,
		logger *zap.Logger,
		queryID string,
		request *api_service_protos.TReadSplitsRequest,
		split *api_service_protos.TSplit,
		sinkFactory paging.SinkFactory[T],
	) error
}

type ColumnDescription struct {
	Name      string // mandatory
	Type      string // mandatory
	Precision *uint8 // filled only for numeric columns
	Scale     *uint8 // filled only for numeric columns
}

type TypeMapper interface {
	SQLTypeToYDBColumn(
		columnDescription *ColumnDescription,
		rules *api_service_protos.TTypeMappingSettings,
	) (*Ydb.Column, error)
}
