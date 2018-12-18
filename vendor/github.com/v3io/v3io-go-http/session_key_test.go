package v3io

import (
	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	"testing"
)

type SessionKeyTestSuite struct {
	suite.Suite
	logger  logger.Logger
	context *Context
	session *Session
}

func (suite *SessionKeyTestSuite) SetupTest() {
	var err error

	suite.logger, err = nucliozap.NewNuclioZapTest("test")

	suite.context, err = NewContext(suite.logger, "<CLUSTER_URL>", 1)
	suite.Require().NoError(err, "Failed to create context")

	sessionConfig := &SessionConfig{
		sessionKey: "<SESSION_KEY>",
	}

	suite.session, err = suite.context.NewSessionFromConfig(sessionConfig)
	suite.Require().NoError(err, "Failed to create session")
}

func (suite *SessionKeyTestSuite) TestListAll() {

	dummyContext := "context"
	responseChan := make(chan *Response, 128)

	request, err := suite.session.ListAll(&ListAllInput{}, &dummyContext, responseChan)
	suite.Require().NoError(err, "List All returned error")
	suite.Require().NotNil(request)

	// read a response
	response := <-responseChan

	// verify there's no error
	suite.Require().NoError(response.Error)

	output, ok := response.Output.(*ListAllOutput)

	suite.Require().True(ok, "Should have been 'ListAllOutput' got %T", response.Output)
	suite.Require().True(len(output.Buckets.Bucket) > 0, "Must have at least one bucket")

	response.Release()
}

func TestSessionKey(t *testing.T) {
	suite.Run(t, new(SessionKeyTestSuite))
}
