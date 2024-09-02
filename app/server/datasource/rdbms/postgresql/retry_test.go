package postgresql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:lll,revive
func TestDeadlineExceeded(t *testing.T) {
	err := errors.New("make connection: connect config: failed to connect to `host=172.24.0.3 user=user database=join_pg_pg`: dial error (timeout: context deadline exceeded)")
	require.True(t, ErrorCheckerMakeConnection(err))
}
