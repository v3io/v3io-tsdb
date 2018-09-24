package v3io

import (
	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	"testing"
)

type SyncSessionTestSuite struct {
	suite.Suite
	logger  logger.Logger
	context *Context
	session *Session
}

func (suite *SyncSessionTestSuite) SetupTest() {
	var err error

	suite.logger, err = nucliozap.NewNuclioZapTest("test")

	suite.context, err = NewContext(suite.logger, "<value>", 1)
	suite.Require().NoError(err, "Failed to create context")

	suite.session, err = suite.context.NewSession("iguazio", "<value>", "")
	suite.Require().NoError(err, "Failed to create session")
}

func (suite *SyncSessionTestSuite) TestListAll() {
	response, err := suite.session.Sync.ListAll()
	suite.Require().NoError(err, "List All returned error")

	output, ok := response.Output.(*ListAllOutput)

	suite.Require().True(ok, "Should have been 'ListAllOutput' got %T", response.Output)
	suite.Require().True(len(output.Buckets.Bucket) > 0, "Must have at least one bucket")

	response.Release()
}

func TestSyncSession(t *testing.T) {
	suite.Run(t, new(SyncSessionTestSuite))
}
