package common

import (
	"errors"
	"fmt"

	ch_proto "github.com/ClickHouse/ch-go/proto"
	clickhouse_proto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/jackc/pgx/v5/pgconn"
	ydb_proto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var (
	ErrTableDoesNotExist                   = fmt.Errorf("table does not exist")
	ErrDataSourceNotSupported              = fmt.Errorf("data source not supported")
	ErrDataTypeNotSupported                = fmt.Errorf("data type not supported")
	ErrMethodNotSupported                  = fmt.Errorf("method not supported")
	ErrReadLimitExceeded                   = fmt.Errorf("read limit exceeded")
	ErrInvalidRequest                      = fmt.Errorf("invalid request")
	ErrValueOutOfTypeBounds                = fmt.Errorf("value is out of possible range of values for the type")
	ErrUnimplemented                       = fmt.Errorf("unimplemented")
	ErrUnimplementedTypedValue             = fmt.Errorf("unimplemented typed value")
	ErrUnimplementedExpression             = fmt.Errorf("unimplemented expression")
	ErrUnsupportedExpression               = fmt.Errorf("expression is not supported")
	ErrUnimplementedOperation              = fmt.Errorf("unimplemented operation")
	ErrUnimplementedPredicateType          = fmt.Errorf("unimplemented predicate type")
	ErrInvariantViolation                  = fmt.Errorf("implementation error (invariant violation)")
	ErrUnimplementedArithmeticalExpression = fmt.Errorf("unimplemented arithmetical expression")
	ErrEmptyTableName                      = fmt.Errorf("empty table name")
	ErrPageSizeExceeded                    = fmt.Errorf("page size exceeded, check service configuration")
)

func NewSuccess() *api_service_protos.TError {
	return &api_service_protos.TError{
		Status:  ydb_proto.StatusIds_SUCCESS,
		Message: "succeeded",
	}
}

func IsSuccess(apiErr *api_service_protos.TError) bool {
	if apiErr == nil {
		return true
	}

	if apiErr.Status == ydb_proto.StatusIds_STATUS_CODE_UNSPECIFIED {
		panic("status uninitialized")
	}

	return apiErr.Status == ydb_proto.StatusIds_SUCCESS
}

//nolint:gocyclo
func NewAPIErrorFromStdError(err error) *api_service_protos.TError {
	if err == nil {
		panic("nil error")
	}

	var status ydb_proto.StatusIds_StatusCode

	// check datasource-specific errors

	chErr := &clickhouse_proto.Exception{}
	if errors.As(err, &chErr) {
		switch chErr.Code {
		case int32(ch_proto.ErrAuthenticationFailed):
			status = ydb_proto.StatusIds_UNAUTHORIZED
		default:
			status = ydb_proto.StatusIds_INTERNAL_ERROR
		}

		return &api_service_protos.TError{
			Status:  status,
			Message: chErr.Message,
		}
	}

	pgConnectError := &pgconn.ConnectError{}
	if errors.As(err, &pgConnectError) {
		pgError, ok := pgConnectError.Unwrap().(*pgconn.PgError)
		if ok {
			// Hopefully these code will be exported some day
			// https://github.com/jackc/pgx/issues/1984
			switch pgError.Code {
			case "28P01":
				status = ydb_proto.StatusIds_UNAUTHORIZED
			default:
				status = ydb_proto.StatusIds_INTERNAL_ERROR
			}

			return &api_service_protos.TError{
				Status:  status,
				Message: chErr.Message,
			}
		}
	}

	if ydb.IsYdbError(err) {
		for status := range ydb_proto.StatusIds_StatusCode_name {
			if ydb.IsOperationError(err, ydb_proto.StatusIds_StatusCode(status)) {
				return &api_service_protos.TError{
					Status:  ydb_proto.StatusIds_StatusCode(status),
					Message: chErr.Message,
				}
			}
		}
	}

	// check general errors that could happen within connector logic

	switch {
	case errors.Is(err, ErrTableDoesNotExist):
		status = ydb_proto.StatusIds_NOT_FOUND
	case errors.Is(err, ErrReadLimitExceeded):
		// Return BAD_REQUEST to avoid retrying
		status = ydb_proto.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrInvalidRequest):
		status = ydb_proto.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrDataSourceNotSupported):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrDataTypeNotSupported):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrValueOutOfTypeBounds):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedTypedValue):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedExpression):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedOperation):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedPredicateType):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplemented):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedArithmeticalExpression):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrEmptyTableName):
		status = ydb_proto.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrPageSizeExceeded):
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	default:
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: err.Error(),
	}
}

func APIErrorToLogFields(apiErr *api_service_protos.TError) []zap.Field {
	return []zap.Field{
		zap.String("message", apiErr.Message),
		zap.String("status", apiErr.Status.String()),
	}
}

func NewSTDErrorFromAPIError(apiErr *api_service_protos.TError) error {
	if IsSuccess(apiErr) {
		return nil
	}

	return errors.New(apiErr.Message)
}

func AllStreamResponsesSuccessfull[T StreamResponse](responses []T) bool {
	for _, resp := range responses {
		if resp.GetError().Status != ydb_proto.StatusIds_SUCCESS {
			return false
		}
	}

	return true
}
