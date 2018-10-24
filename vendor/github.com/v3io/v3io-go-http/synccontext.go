package v3io

import (
	"time"

	"github.com/nuclio/logger"
	"github.com/valyala/fasthttp"
)

type SyncContext struct {
	logger     logger.Logger
	httpClient *fasthttp.HostClient
	clusterURL string
	Timeout    time.Duration
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

	if sc.Timeout <= 0 {
		return sc.httpClient.Do(request, response)
	} else {
		return sc.httpClient.DoTimeout(request, response, sc.Timeout)
	}
}
