package logging

import (
	"fmt"
	"time"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.FilterChecker = (*filterCheckerImpl)(nil)

type filterCheckerImpl struct {
	maxRangeDuration time.Duration
}

// NewFilterChecker creates a new instance of FilterChecker for logging datasource.
func NewFilterChecker(maxRangeDuration time.Duration) rdbms_utils.FilterChecker {
	return &filterCheckerImpl{
		maxRangeDuration: maxRangeDuration,
	}
}

// CheckFilter checks if the predicate contains a filter over the timestamp column.
func (f filterCheckerImpl) CheckFilter(where *api_service_protos.TSelect_TWhere) error {
	if where == nil || where.FilterTyped == nil {
		return fmt.Errorf("empty predicate is not allowed")
	}

	// Check if timestamp filter exists
	hasTimestampFilter, err := checkTimestampFilter(where.FilterTyped)
	if err != nil {
		return fmt.Errorf("check timestamp filter for value %v: %w", where.FilterTyped, err)
	}

	if !hasTimestampFilter {
		return fmt.Errorf("filter over 'timestamp' column is required: %w", common.ErrBadRequest)
	}

	return nil
}

// checkTimestampFilter traverses the predicate tree to check if a filter over the timestamp column exists.
func checkTimestampFilter(predicate *api_service_protos.TPredicate) (bool, error) {
	if predicate == nil {
		return false, nil
	}

	switch {
	case predicate.GetConjunction() != nil:
		// For AND conditions, check all operands
		for _, operand := range predicate.GetConjunction().GetOperands() {
			found, err := checkTimestampFilter(operand)
			if err != nil {
				return false, fmt.Errorf("check timestamp filter for AND operand %v: %w", operand, err)
			}

			if found {
				return true, nil
			}
		}

	case predicate.GetDisjunction() != nil:
		// For OR conditions, check all operands
		for _, operand := range predicate.GetDisjunction().GetOperands() {
			found, err := checkTimestampFilter(operand)
			if err != nil {
				return false, fmt.Errorf("check timestamp filter for OR operand %v: %w", operand, err)
			}

			if found {
				return true, nil
			}
		}

	case predicate.GetNegation() != nil:
		// Check the negated operand
		found, err := checkTimestampFilter(predicate.GetNegation().GetOperand())
		if err != nil {
			return false, fmt.Errorf(
				"check timestamp filter for NEGATION operand %v: %w",
				 predicate.GetNegation().GetOperand(), 
				 err,
				)
		}

		return found, nil

	case predicate.GetComparison() != nil:
		// Check if this is a timestamp column comparison
		comparison := predicate.GetComparison()
		leftExpr := comparison.GetLeftValue()

		// Check if we're comparing a column named "timestamp"
		if leftExpr != nil && leftExpr.GetColumn() == "timestamp" {
			return true, nil
		}

	case predicate.GetCoalesce() != nil:
		// For coalesce, check all operands
		for _, operand := range predicate.GetCoalesce().GetOperands() {
			found, err := checkTimestampFilter(operand)
			if err != nil {
				return false, fmt.Errorf("check timestamp filter for COALESCE operand %v: %w", operand, err)
			}

			if found {
				return true, nil
			}
		}
	}

	return false, nil
}
