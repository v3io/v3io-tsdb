package v3io

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"

	"github.com/nuclio/logger"
	"github.com/valyala/fasthttp"
)

type SyncSession struct {
	logger                   logger.Logger
	context                  *SyncContext
	authenticatioHeaderKey   string
	authenticatioHeaderValue string
}

func newSyncSession(parentLogger logger.Logger,
	context *SyncContext,
	username string,
	password string,
	label string,
	sessionKey string) (*SyncSession, error) {

	if sessionKey != "" {
		//if sessionKey not empty
		return &SyncSession{
			logger:                   parentLogger.GetChild("session"),
			context:                  context,
			authenticatioHeaderKey:   "X-v3io-session-key",
			authenticatioHeaderValue: sessionKey,
		}, nil
	}
	// generate token for basic authentication
	usernameAndPassword := fmt.Sprintf("%s:%s", username, password)
	encodedUsernameAndPassword := base64.StdEncoding.EncodeToString([]byte(usernameAndPassword))

	return &SyncSession{
		logger:                   parentLogger.GetChild("session"),
		context:                  context,
		authenticatioHeaderKey:   "Authorization",
		authenticatioHeaderValue: "Basic " + encodedUsernameAndPassword,
	}, nil
}

func (ss *SyncSession) ListAll() (*Response, error) {
	output := ListAllOutput{}

	return ss.sendRequestAndXMLUnmarshal("GET", fmt.Sprintf("http://%s/", ss.context.clusterURL), nil, nil, &output)
}

func (ss *SyncSession) sendRequestViaContext(request *fasthttp.Request, response *fasthttp.Response) error {

	request.Header.Set(ss.authenticatioHeaderKey, ss.authenticatioHeaderValue)

	// delegate to context
	return ss.context.sendRequest(request, response)
}

func (ss *SyncSession) sendRequest(
	method string,
	uri string,
	headers map[string]string,
	body []byte,
	releaseResponse bool) (*Response, error) {

	var success bool
	var statusCode int

	request := fasthttp.AcquireRequest()
	response := allocateResponse()

	// init request
	request.SetRequestURI(uri)
	request.Header.SetMethod(method)
	request.SetBody(body)

	if headers != nil {
		for headerName, headerValue := range headers {
			request.Header.Add(headerName, headerValue)
		}
	}

	// execute the request
	err := ss.sendRequestViaContext(request, response.response)
	if err != nil {
		goto cleanup
	}

	statusCode = response.response.StatusCode()

	// did we get a 2xx response?
	success = statusCode >= 200 && statusCode < 300

	// make sure we got expected status
	if !success {
		err = NewErrorWithStatusCode(statusCode, "Failed %s with status %d", method, statusCode)
		goto cleanup
	}

cleanup:

	// we're done with the request - the response must be released by the user
	// unless there's an error
	fasthttp.ReleaseRequest(request)

	if err != nil {
		response.Release()
		return nil, err
	}

	// if the user doesn't need the response, release it
	if releaseResponse {
		response.Release()
		return nil, nil
	}

	return response, nil
}

func (ss *SyncSession) sendRequestAndXMLUnmarshal(
	method string,
	uri string,
	headers map[string]string,
	body []byte,
	output interface{}) (*Response, error) {

	response, err := ss.sendRequest(method, uri, headers, body, false)
	if err != nil {
		return nil, err
	}

	// unmarshal the body into the output
	err = xml.Unmarshal(response.response.Body(), output)
	if err != nil {
		response.Release()

		return nil, err
	}

	// set output in response
	response.Output = output

	return response, nil
}
