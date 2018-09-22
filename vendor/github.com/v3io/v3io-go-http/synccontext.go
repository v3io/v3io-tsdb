package v3io

import (
	"github.com/nuclio/logger"
	"github.com/valyala/fasthttp"
)

type SyncContext struct {
	logger     logger.Logger
	httpClient *fasthttp.HostClient
	clusterURL string
}

func newSyncContext(parentLogger logger.Logger, clusterURL string) (*SyncContext, error) {
	newSyncContext := &SyncContext{
		logger: parentLogger.GetChild("v3io"),
		httpClient: &fasthttp.HostClient{
			Addr: clusterURL,
		},
		clusterURL: clusterURL,
	}

	return newSyncContext, nil
}

func (sc *SyncContext) sendRequest(request *fasthttp.Request, response *fasthttp.Response) error {

	err := sc.httpClient.Do(request, response)
	if err != nil {
		return err
	}

	return nil
}
