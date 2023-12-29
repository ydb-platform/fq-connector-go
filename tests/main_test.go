package tests

import (
	"log"
	"testing"
	"time"

	testify_suite "github.com/stretchr/testify/suite"

	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

// TODO: find the way of passing this object into suites as a parameter instead of global var
var state *suite.State

func TestMain(m *testing.M) {
	var err error

	state, err = suite.NewState()
	if err != nil {
		log.Fatal(err)
	}

	// Pause to let datasources start
	// TODO: ping ports
	time.Sleep(3 * time.Second)

	m.Run()
}

func TestSelect(t *testing.T) { testify_suite.Run(t, NewSelectSuite(suite.NewBase(t, state))) }
