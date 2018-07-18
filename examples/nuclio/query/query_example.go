package query

import (
	"bytes"
	"encoding/json"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
)

// Configuration
const tsdbConfig = `
path: "pmetric"
`

type tsdbQuery struct {
	Name        string
	Aggregators []string
	Step        string
	Filter      string
	From        string
	To          string
	Last        string
}

// example query event
const queryEvent = `
{
  "Name": "cpu", 
  "Last":"2h"
}
`

// Note: the user must define the v3io data binding in the nuclio function with path, username, password and name it db0

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	query := tsdbQuery{}
	err := json.Unmarshal(event.GetBody(), &query)
	if err != nil {
		return nil, err
	}

	// convert string times (unix or RFC3339 or relative like now-2h) to unix milisec times
	from, to, step, err := utils.GetTimeFromRange(query.From, query.To, query.Last, query.Step)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing query time range")
	}

	// Create TSDB Querier
	context.Logger.DebugWith("Query", "params", query)
	adapter := context.UserData.(*tsdb.V3ioAdapter)
	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Querier")
	}

	// Select Query to get back a series set iterator
	set, err := qry.Select(query.Name, strings.Join(query.Aggregators, ","), step, query.Filter)
	if err != nil {
		return nil, errors.Wrap(err, "Select Failed")
	}

	// convert SeriesSet to Json (Grafana simpleJson format)
	f, err := formatter.NewFormatter("json", nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start json formatter")
	}

	var b bytes.Buffer
	err = f.Write(&b, set)

	return b.String(), err
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {
	cfg, _ := config.LoadFromData([]byte(tsdbConfig))
	data := context.DataBinding["db0"].(*v3io.Container)
	adapter, err := tsdb.NewV3ioAdapter(cfg, data, context.Logger)
	if err != nil {
		return err
	}

	// Store adapter in user cache
	context.UserData = adapter
	return nil
}
