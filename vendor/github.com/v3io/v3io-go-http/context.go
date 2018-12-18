package v3io

import (
	"github.com/nuclio/logger"
)

type Context struct {
	logger      logger.Logger
	Sync        *SyncContext
	requestChan chan *Request
	numWorkers  int
}

type SessionConfig struct {
	Username	string
	Password	string
	Label		string
	SessionKey	string
}

func NewContext(parentLogger logger.Logger, clusterURL string, numWorkers int) (*Context, error) {
	newSyncContext, err := newSyncContext(parentLogger, clusterURL)
	if err != nil {
		return nil, err
	}

	newContext := &Context{
		logger:      parentLogger.GetChild("v3io"),
		Sync:        newSyncContext,
		requestChan: make(chan *Request, 1024),
		numWorkers:  numWorkers,
	}

	for workerIndex := 0; workerIndex < numWorkers; workerIndex++ {
		go newContext.workerEntry(workerIndex)
	}

	return newContext, nil
}

func (c *Context) NewSession(username string, password string, label string) (*Session, error) {
	return newSession(c.logger, c, username, password, label, "")
}

func (c *Context) NewSessionFromConfig(sc *SessionConfig) (*Session, error) {
	return newSession(c.logger, c, sc.Username, sc.Password, sc.Label, sc.SessionKey)
}

func (c *Context) sendRequest(request *Request) error {

	// send the request to the request channel
	c.requestChan <- request

	return nil
}

func (c *Context) workerEntry(workerIndex int) {
	for {
		var response *Response
		var err error

		// read a request
		request := <-c.requestChan

		// according to the input type
		switch typedInput := request.Input.(type) {
		case *ListAllInput:
			response, err = request.session.Sync.ListAll()
		case *ListBucketInput:
			response, err = request.container.Sync.ListBucket(typedInput)
		case *GetObjectInput:
			response, err = request.container.Sync.GetObject(typedInput)
		case *PutObjectInput:
			err = request.container.Sync.PutObject(typedInput)
		case *DeleteObjectInput:
			err = request.container.Sync.DeleteObject(typedInput)
		case *GetItemInput:
			response, err = request.container.Sync.GetItem(typedInput)
		case *GetItemsInput:
			response, err = request.container.Sync.GetItems(typedInput)
		case *PutItemInput:
			err = request.container.Sync.PutItem(typedInput)
		case *PutItemsInput:
			response, err = request.container.Sync.PutItems(typedInput)
		case *UpdateItemInput:
			err = request.container.Sync.UpdateItem(typedInput)
		case *CreateStreamInput:
			err = request.container.Sync.CreateStream(typedInput)
		case *DeleteStreamInput:
			err = request.container.Sync.DeleteStream(typedInput)
		case *SeekShardInput:
			response, err = request.container.Sync.SeekShard(typedInput)
		case *PutRecordsInput:
			response, err = request.container.Sync.PutRecords(typedInput)
		case *GetRecordsInput:
			response, err = request.container.Sync.GetRecords(typedInput)
		default:
			c.logger.ErrorWith("Got unexpected request type", "request", request)
		}

		// TODO: have the sync interfaces somehow use the pre-allocated response
		if response != nil {
			request.requestResponse.Response = *response
		}

		response = &request.requestResponse.Response

		response.ID = request.ID
		response.Error = err
		response.requestResponse = request.requestResponse
		response.Context = request.Context

		// write to response channel
		request.responseChan <- &request.requestResponse.Response
	}
}
