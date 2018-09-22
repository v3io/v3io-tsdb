package v3io

import (
	"sync/atomic"

	"github.com/nuclio/logger"
)

// TODO: Request should have a global pool
var requestID uint64 = 0

type Session struct {
	Sync    *SyncSession
	logger  logger.Logger
	context *Context
}

func newSession(parentLogger logger.Logger,
	context *Context,
	username string,
	password string,
	label string) (*Session, error) {

	newSyncSession, err := newSyncSession(parentLogger, context.Sync, username, password, label)
	if err != nil {
		return nil, err
	}

	return &Session{
		logger:  parentLogger.GetChild("session"),
		context: context,
		Sync:    newSyncSession,
	}, nil
}

func (s *Session) NewContainer(alias string) (*Container, error) {
	return newContainer(s.logger, s, alias)
}

func (s *Session) ListAll(input *ListAllInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {

	id := atomic.AddUint64(&requestID, 1)
	requestResponse := &RequestResponse{
		Request: Request{
			ID:           id,
			Input:        input,
			Context:      context,
			responseChan: responseChan,
		},
	}

	// point to container
	requestResponse.Request.requestResponse = requestResponse

	if err := s.sendRequest(&requestResponse.Request); err != nil {
		return nil, err
	}

	return &requestResponse.Request, nil
}

func (s *Session) sendRequest(request *Request) error {

	// set session
	request.session = s

	// delegate to context
	return s.context.sendRequest(request)
}
