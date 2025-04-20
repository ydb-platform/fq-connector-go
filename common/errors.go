package common

import (
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	ch_proto "github.com/ClickHouse/ch-go/proto"
	clickhouse_proto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	grpc_codes "google.golang.org/grpc/codes"

	ydb_proto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
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
	// TODO: remove this and extract OracleError somehow
	oracleRegex = regexp.MustCompile(`ORA-(\d+):`)
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

func newAPIErrorFromOracleError(err error) *api_service_protos.TError {
	// go-ora error mapping:
	// https://github.com/sijms/go-ora/blob/v2.8.19/v2/network/oracle_error.go#L20-L57
	var status ydb_proto.StatusIds_StatusCode

	// TODO: remove this and extract OracleError somehow
	//       errors.As() does not work with go_ora.OracleError because it does not implement Error interface
	errorText := err.Error()

	match := oracleRegex.FindStringSubmatch(errorText)

	if len(match) != 2 {
		return nil
	}

	tmp, err := strconv.ParseUint(match[1], 10, 16)
	if err != nil {
		panic(fmt.Errorf("API error from Oracle error: %w", err))
	}

	code := uint16(tmp)

	switch code {
	case 1017: // ORA-01017: invalid username/password
		status = ydb_proto.StatusIds_UNAUTHORIZED
	case 12514: // ORA-12514 TNS: ... --- wrong SERVICE_NAME
		status = ydb_proto.StatusIds_NOT_FOUND
	// TODO: more codes from go-ora error mapping or Oracle docs
	default:
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: errorText,
	}
}

func newAPIErrorFromMsSQLServer(err error) *api_service_protos.TError {
	var (
		target mssql.Error
		status ydb_proto.StatusIds_StatusCode
	)

	if errors.As(err, &target) {
		switch {
		case strings.Contains(target.Message, "Login failed"):
			status = ydb_proto.StatusIds_UNAUTHORIZED
		default:
			status = ydb_proto.StatusIds_INTERNAL_ERROR
		}

		return &api_service_protos.TError{
			Status:  status,
			Message: err.Error(),
		}
	}

	return nil
}

func newAPIErrorFromMySQLError(err error) *api_service_protos.TError {
	var status ydb_proto.StatusIds_StatusCode

	// TODO: remove this and extract MyError somehow
	//       for some reason errors.As() does not work with mysql.MyError
	errorText := err.Error()

	var code uint16

	match := mysqlRegex.FindString(errorText)

	if len(match) > 0 {
		tmp, err := strconv.ParseUint(match, 10, 16)
		if err != nil {
			panic(err)
		}

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

func newAPIErrorFromTLSError(err error) *api_service_protos.TError {
	var (
		// status ydb_proto.StatusIds_StatusCode  // TODO: parse specific TLS errors
		// unknownAuthorityError   = &x509.UnknownAuthorityError{}
		// certificateInvalidError = &x509.CertificateInvalidError{}
		// hostnameError           = &x509.HostnameError{}
		// systemRooteError      = &x509.SystemRootsError{}
		certVerificationError = &tls.CertificateVerificationError{} // tls.CertificateVerificationError wraps all the x509 errors
	)

	if errors.As(err, &certVerificationError) {
		return &api_service_protos.TError{
			Status:  ydb_proto.StatusIds_UNAVAILABLE,
			Message: err.Error(),
		}
	}

	return nil
}

func newAPIErrorFromMongoDbError(err error) *api_service_protos.TError {
	if err == nil {
		return nil
	}

	// https://www.mongodb.com/docs/manual/reference/error-codes/
	const (
		hostNotFound         = 7
		unauthorized         = 13
		authenticationFailed = 18
	)

	var status ydb_proto.StatusIds_StatusCode

	if mongo.IsTimeout(err) {
		status = ydb_proto.StatusIds_TIMEOUT
	} else if e, ok := err.(mongo.ServerError); ok {
		if e.HasErrorCode(hostNotFound) {
			status = ydb_proto.StatusIds_NOT_FOUND
		} else if e.HasErrorCode(unauthorized) || e.HasErrorCode(authenticationFailed) {
			status = ydb_proto.StatusIds_UNAUTHORIZED
		} else {
			status = ydb_proto.StatusIds_INTERNAL_ERROR
		}
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: err.Error(),
	}
}

func newAPIErrorFromRedisError(err error) *api_service_protos.TError {
	if err == nil {
		return nil
	}

	var status ydb_proto.StatusIds_StatusCode

	// Если ошибка равна redis.Nil, то, возможно, ключ не найден.
	if errors.Is(err, redis.Nil) {
		status = ydb_proto.StatusIds_NOT_FOUND
	} else if strings.Contains(err.Error(), "NOAUTH") || strings.Contains(err.Error(), "WRONGPASS") {
		// Ошибка аутентификации
		status = ydb_proto.StatusIds_UNAUTHORIZED
	} else if strings.Contains(err.Error(), "LOADING") {
		// Redis может возвращать ошибку LOADING, если данные загружаются из диска.
		status = ydb_proto.StatusIds_UNAVAILABLE
	} else {
		// По умолчанию считаем, что это внутренняя ошибка.
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: err.Error(),
	}
}

func newAPIErrorFromOpenSearchError(err error) *api_service_protos.TError {
	if err == nil {
		return nil
	}

	var status ydb_proto.StatusIds_StatusCode

	errMsg := err.Error()

	switch {
	case strings.Contains(errMsg, "connection refused") || strings.Contains(errMsg, "no such host"):
		status = ydb_proto.StatusIds_UNAVAILABLE
	case strings.Contains(errMsg, "401 Unauthorized"):
		status = ydb_proto.StatusIds_UNAUTHORIZED
	case strings.Contains(errMsg, "index_not_found_exception"):
		status = ydb_proto.StatusIds_NOT_FOUND
	case strings.Contains(errMsg, "parsing_exception") || strings.Contains(errMsg, "illegal_argument_exception"):
		status = ydb_proto.StatusIds_BAD_REQUEST
	case strings.Contains(errMsg, "cluster_block_exception"):
		status = ydb_proto.StatusIds_UNAVAILABLE
	default:
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: errMsg,
	}
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
	case errors.Is(err, ErrUnimplementedArithmeticalExpression):
		status = ydb_proto.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrEmptyTableName):
		status = ydb_proto.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrPageSizeExceeded):
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	case errors.Is(err, ErrUnsupportedExpression):
		status = ydb_proto.StatusIds_UNSUPPORTED
	default:
		status = ydb_proto.StatusIds_INTERNAL_ERROR
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: err.Error(),
	}
}

func NewAPIErrorFromStdError(err error, kind api_common.EGenericDataSourceKind) *api_service_protos.TError {
	if err == nil {
		panic("nil error")
	}

	// check datasource-specific errors
	var apiError *api_service_protos.TError

	switch kind {
	case api_common.EGenericDataSourceKind_DATA_SOURCE_KIND_UNSPECIFIED:
	case api_common.EGenericDataSourceKind_CLICKHOUSE:
		apiError = newAPIErrorFromClickHouseError(err)
	case api_common.EGenericDataSourceKind_POSTGRESQL, api_common.EGenericDataSourceKind_GREENPLUM:
		apiError = newAPIErrorFromPostgreSQLError(err)
	case api_common.EGenericDataSourceKind_MYSQL:
		apiError = newAPIErrorFromMySQLError(err)
	case api_common.EGenericDataSourceKind_YDB:
		apiError = newAPIErrorFromYdbError(err)
	case api_common.EGenericDataSourceKind_ORACLE:
		apiError = newAPIErrorFromOracleError(err)
	case api_common.EGenericDataSourceKind_MS_SQL_SERVER:
		apiError = newAPIErrorFromMsSQLServer(err)
	case api_common.EGenericDataSourceKind_LOGGING:
		apiError = newAPIErrorFromYdbError(err)
	case api_common.EGenericDataSourceKind_MONGO_DB:
		apiError = newAPIErrorFromMongoDbError(err)
	case api_common.EGenericDataSourceKind_REDIS:
		apiError = newAPIErrorFromRedisError(err)
	case api_common.EGenericDataSourceKind_OPENSEARCH:
		apiError = newAPIErrorFromOpenSearchError(err)
	default:
		panic(fmt.Sprintf("Unexpected data source kind: %v", api_common.EGenericDataSourceKind_name[int32(kind)]))
	}

	if apiError != nil {
		return apiError
	}

	apiError = newAPIErrorFromTLSError(err)
	if apiError != nil {
		return apiError
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

type ErrorMatcher struct {
	errors []error
}

func (m *ErrorMatcher) Match(err error) bool {
	for _, e := range m.errors {
		if errors.Is(err, e) {
			return true
		}
	}

	return false
}

func NewErrorMatcher(errs ...error) *ErrorMatcher {
	return &ErrorMatcher{errs}
}
