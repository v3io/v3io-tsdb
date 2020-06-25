package tsdbctl

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type testDeleteSuite struct {
	suite.Suite
}

func (suite *testDeleteSuite) TestNewDeleteCommandeerErrorOnAllAndBeginConflict() {
	commandeer := newDeleteCommandeer(&RootCommandeer{})
	commandeer.deleteAll = true
	commandeer.fromTime = "123"
	err := commandeer.cmd.RunE(commandeer.cmd, nil)
	suite.Error(err)
	suite.Equal("delete --all cannot be used in conjunction with --begin", err.Error())
}

func (suite *testDeleteSuite) TestNewDeleteCommandeerErrorOnAllAndFilterConflict() {
	commandeer := newDeleteCommandeer(&RootCommandeer{})
	commandeer.deleteAll = true
	commandeer.filter = "some filter"
	err := commandeer.cmd.RunE(commandeer.cmd, nil)
	suite.Error(err)
	suite.Equal("delete --all cannot be used in conjunction with --filter", err.Error())
}

func TestDeleteSuite(t *testing.T) {
	suite.Run(t, new(testDeleteSuite))
}
