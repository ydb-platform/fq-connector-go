package suite

import (
	"fmt"
	"testing"

	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/infra/connector"
)

type Base struct {
	testify_suite.Suite
	*State
	Connector *connector.Server
	name      string
}

func (b *Base) SetupSuite() {
	fmt.Printf("\n>>>>>>>>>> Suite started: %s <<<<<<<<<<\n", b.name)

	// We want to run a distinct instance of Connector for every suite
	var err error
	b.Connector, err = connector.NewServer()
	b.Require().NoError(err)
	b.Connector.Start()
}

func (b *Base) TearDownSuite() {
	b.Connector.Stop()

	fmt.Printf("\n>>>>>>>>>> Suite stopped: %s <<<<<<<<<<\n", b.name)
}

func NewBase(t *testing.T, state *State, name string) *Base {
	b := &Base{
		State: state,
		name:  name,
	}

	b.SetT(t)

	return b
}
