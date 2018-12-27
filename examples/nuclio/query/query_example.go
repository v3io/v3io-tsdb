package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"

	"github.com/nuclio/nuclio-sdk-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// Example request:
//
// {
//     "metric": "cpu",
//     "step": "1m",
//     "start_time": "1532095945142",
//     "end_time": "1642995948517"
// }

type request struct {
	Metric           string   `json:"metric"`
	Aggregators      []string `json:"aggregators"`
	FilterExpression string   `json:"filter_expression"`
	Step             string   `json:"step"`
	StartTime        string   `json:"start_time"`
	EndTime          string   `json:"end_time"`
	Last             string   `json:"last"`
}

type userData struct {
	querier *pquerier.V3ioQuerier
}

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	request := request{}

	// try to unmarshal the request. return bad request if failed
	if err := json.Unmarshal(event.GetBody(), &request); err != nil {
		return nil, nuclio.WrapErrBadRequest(err)
	}

	context.Logger.DebugWith("Got query request", "request", request)

	// convert string times (unix or RFC3339 or relative like now-2h) to unix milisec times
	from, to, step, err := utils.GetTimeFromRange(request.StartTime, request.EndTime, request.Last, request.Step)
	if err != nil {
		return nil, nuclio.WrapErrBadRequest(errors.Wrap(err, "Error parsing query time range"))
	}

	// get user data from context, as initialized by InitContext
	tsdbQuerier := context.UserData.(*userData).querier

	params := &pquerier.SelectParams{Name: request.Metric,
		Functions: strings.Join(request.Aggregators, ","),
		Step:      step,
		Filter:    request.FilterExpression,
		From:      from,
		To:        to}
	// Select query to get back a series set iterator
	seriesSet, err := tsdbQuerier.Select(params)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to execute query select")
	}

	// convert SeriesSet to JSON (Grafana simpleJson format)
	jsonFormatter, err := formatter.NewFormatter("json", nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start json formatter")
	}

	var buffer bytes.Buffer
	err = jsonFormatter.Write(&buffer, seriesSet)

	return buffer.String(), err
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {

	// get configuration from env
	v3ioAdapterPath := os.Getenv("QUERY_V3IO_TSDB_PATH")
	if v3ioAdapterPath == "" {
		return errors.New("QUERY_V3IO_TSDB_PATH must be set")
	}

	context.Logger.InfoWith("Initializing", "v3ioAdapterPath", v3ioAdapterPath)

	// create v3io adapter
	tsdbQuerier, err := createV3ioQuerier(context, v3ioAdapterPath)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize querier")
	}
	context.UserData = userData{tsdbQuerier}
	return nil
}

func createV3ioQuerier(context *nuclio.Context, path string) (*pquerier.V3ioQuerier, error) {
	context.Logger.InfoWith("Creating v3io adapter", "path", path)

	var err error

	v3ioConfig, err := config.GetOrLoadFromStruct(&config.V3ioConfig{
		TablePath: path,
	})
	if err != nil {
		return nil, err
	}
	container, err := tsdb.NewContainerFromEnv(context.Logger)
	if err != nil {
		return nil, err
	}
	// create adapter once for all contexts
	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, container, context.Logger)
	if err != nil {
		return nil, err
	}
	// Create TSDB Querier
	querierInstance, err := adapter.QuerierV2()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize querier")
	}
	return querierInstance, nil
}
