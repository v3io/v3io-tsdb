package v3io

import (
	"sync/atomic"

	"github.com/nuclio/logger"
)

type Container struct {
	logger  logger.Logger
	session *Session
	Sync    *SyncContainer
}

func newContainer(parentLogger logger.Logger, session *Session, alias string) (*Container, error) {
	newSyncContainer, err := newSyncContainer(parentLogger, session.Sync, alias)
	if err != nil {
		return nil, err
	}

	return &Container{
		logger:  parentLogger.GetChild(alias),
		session: session,
		Sync:    newSyncContainer,
	}, nil
}

func (c *Container) ListAll(input *ListAllInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) ListBucket(input *ListBucketInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) GetObject(input *GetObjectInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) DeleteObject(input *DeleteObjectInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) PutObject(input *PutObjectInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) GetItem(input *GetItemInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) GetItems(input *GetItemsInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) PutItem(input *PutItemInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) PutItems(input *PutItemsInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) UpdateItem(input *UpdateItemInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) CreateStream(input *CreateStreamInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) DeleteStream(input *DeleteStreamInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) SeekShard(input *SeekShardInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) PutRecords(input *PutRecordsInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) GetRecords(input *GetRecordsInput,
	context interface{},
	responseChan chan *Response) (*Request, error) {
	return c.sendRequest(input, context, responseChan)
}

func (c *Container) sendRequest(input interface{},
	context interface{},
	responseChan chan *Response) (*Request, error) {
	id := atomic.AddUint64(&requestID, 1)

	// create a request/response (TODO: from pool)
	requestResponse := &RequestResponse{
		Request: Request{
			ID:           id,
			container:    c,
			Input:        input,
			Context:      context,
			responseChan: responseChan,
		},
	}

	// point to container
	requestResponse.Request.requestResponse = requestResponse

	if err := c.session.sendRequest(&requestResponse.Request); err != nil {
		return nil, err
	}

	return &requestResponse.Request, nil
}
