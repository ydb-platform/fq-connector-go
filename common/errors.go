package common

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	ch_proto "github.com/ClickHouse/ch-go/proto"
	clickhouse_proto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	ydb_proto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	grpc_codes "google.golang.org/grpc/codes"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var (
	ErrTableDoesNotExist                   = fmt.Errorf("table does not exist")
	ErrDataSourceNotSupported              = fmt.Errorf("data source not supported")
	ErrDataTypeNotSupported                = fmt.Errorf("data type not supported")
	ErrDataTypeMismatch                    = fmt.Errorf("data type mismatch")
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

var (
	// TODO: remove this and extract MyError somehow
	mysqlRegex = regexp.MustCompile(`\d+`)
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

func newAPIErrorFromClickHouseError(err error) *api_service_protos.TError {
	var (
		status ydb_proto.StatusIds_StatusCode
		chErr  = &clickhouse_proto.Exception{}
	)

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

	return nil
}

func newAPIErrorFromPostgreSQLError(err error) *api_service_protos.TError {
	var (
		status         ydb_proto.StatusIds_StatusCode
		pgConnectError = &pgconn.ConnectError{}
	)

	if errors.As(err, &pgConnectError) {
		pgError, ok := pgConnectError.Unwrap().(*pgconn.PgError)
		if ok {
			switch pgError.Code {
			case pgerrcode.InvalidPassword:
				// Invalid password in PostgreSQL 15
				status = ydb_proto.StatusIds_UNAUTHORIZED
			case pgerrcode.InvalidAuthorizationSpecification:
				// Invalid password in Greenplum 6.25
				status = ydb_proto.StatusIds_UNAUTHORIZED
			default:
				status = ydb_proto.StatusIds_INTERNAL_ERROR
			}

			return &api_service_protos.TError{
				Status:  status,
				Message: pgError.Message,
			}
		}
	}

	return nil
}

func newAPIErrorFromMySQLError(err error) *api_service_protos.TError {
	var status ydb_proto.StatusIds_StatusCode

	// TODO: remove this and extract MyError somehow
	//       for some reason errors.As() does not work with mysql.MyError
	errorText := err.Error()
	if strings.Contains(errorText, "mysql:") {
		var code uint16

		match := mysqlRegex.FindString(errorText)

		if len(match) > 0 {
			tmp, _ := strconv.ParseUint(match, 10, 16)
			code = uint16(tmp)
		}

		switch code {
		case mysql.ER_DBACCESS_DENIED_ERROR, mysql.ER_ACCESS_DENIED_ERROR, mysql.ER_PASSWORD_NO_MATCH:
			status = ydb_proto.StatusIds_UNAUTHORIZED
		case mysql.ER_BAD_DB_ERROR:
			status = ydb_proto.StatusIds_NOT_FOUND
		default:
			status = ydb_proto.StatusIds_INTERNAL_ERROR
		}

		return &api_service_protos.TError{
			Status:  status,
			Message: errorText,
		}
	}

	return nil
}

func newAPIErrorFromYdbError(err error) *api_service_protos.TError {
	if ydb.IsYdbError(err) {
		for status := range ydb_proto.StatusIds_StatusCode_name {
			if ydb.IsOperationError(err, ydb_proto.StatusIds_StatusCode(status)) {
				return &api_service_protos.TError{
					Status:  ydb_proto.StatusIds_StatusCode(status),
					Message: err.Error(),
				}
			}

			if ydb.IsTransportError(err, grpc_codes.ResourceExhausted) {
				return &api_service_protos.TError{
					Status:  ydb_proto.StatusIds_OVERLOADED,
					Message: err.Error(),
				}
			}
		}
	}

	return nil
}

//nolint:gocyclo
func newAPIErrorFromConnectorError(err error) *api_service_protos.TError {
	var status ydb_proto.StatusIds_StatusCode

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
	case errors.Is(err, ErrDataTypeMismatch):
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


func NewAPIErrorFromStdError(err error) *api_service_protos.TError {
	if err == nil {
		panic("nil error")
	}

	// check datasource-specific errors
	if apiErr := newAPIErrorFromClickHouseError(err); apiErr != nil {
		return apiErr
	}

	if apiErr := newAPIErrorFromPostgreSQLError(err); apiErr != nil {
		return apiErr
	}

	if apiErr := newAPIErrorFromMySQLError(err); apiErr != nil {
		return apiErr
	}

	if apiErr := newAPIErrorFromYdbError(err); apiErr != nil {
		return apiErr
	}

	// check general errors that could happen within connector logic
	return newAPIErrorFromConnectorError(err)
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
