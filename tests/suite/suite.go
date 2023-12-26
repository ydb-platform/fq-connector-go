package suite

import (
	"testing"

	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/infra/connector"
)

type Base struct {
	testify_suite.Suite
	*State
	Connector *connector.Server
}

func (b *Base) SetupSuite() {
	// We want to run a distinct instance of Connector for every suite
	var err error
	b.Connector, err = connector.NewServer()
	b.Require().NoError(err)
	b.Connector.Start()
}

func (b *Base) TearDownSuite() {
	b.Connector.Stop()
}

// TODO: pass options in order to parametrize test environment launched in suite setup
func NewBase(t *testing.T, state *State) *Base {
	b := &Base{
		State: state,
	}

	b.SetT(t)

	return b
}
