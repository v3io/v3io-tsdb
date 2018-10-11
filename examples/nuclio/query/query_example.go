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
var tsdbConfig = `
path: "pmetric"
`

type tsdbQuery struct {
	Name       string
	Aggregates []string
	Step       string
	Filter     string
	From       string
	To         string
	Last       string
}

// Example query event
const queryEvent = `
{
  "Name": "cpu", 
  "Last":"2h"
}
`

// Note: The user must define a "db0" V3IO data binding in the nuclio function
// with path, username, and password.

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	query := tsdbQuery{}
	err := json.Unmarshal(event.GetBody(), &query)
	if err != nil {
		return nil, err
	}

	// Convert time strings to Unix timestamp in milliseconds integers.
	// The input time string can be of the format "now", "now-[0-9]+[mdh]"
	// (for example, "now-2h"), "<Unix timestamp in milliseconds>", or
	// "<RFC3339 time>" (for example, "2018-09-26T14:10:20Z").
	from, to, step, err := utils.GetTimeFromRange(query.From, query.To, query.Last, query.Step)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing query time range.")
	}

	// Create a TSDB Querier
	context.Logger.DebugWith("Query", "params", query)
	adapter := context.UserData.(*tsdb.V3ioAdapter)
	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Querier")
	}

	// Select a query to get back a series-set iterator
	set, err := qry.Select(query.Name, strings.Join(query.Aggregates, ","), step, query.Filter)
	if err != nil {
		return nil, errors.Wrap(err, "Select Failed")
	}

	// Convert a SeriesSet to a JSON object (Grafana simpleJson format)
	f, err := formatter.NewFormatter("json", nil)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to start the JSON formatter.")
	}

	var b bytes.Buffer
	err = f.Write(&b, set)

	return b.String(), err
}

// InitContext runs only once, when the function runtime starts
func InitContext(context *nuclio.Context) error {
	cfg, _ := config.GetOrLoadFromData([]byte(tsdbConfig))
	data := context.DataBinding["db0"].(*v3io.Container)
	adapter, err := tsdb.NewV3ioAdapter(cfg, data, context.Logger)
	if err != nil {
		return err
	}

	// Store the adapter in the user-data cache
	context.UserData = adapter
	return nil
}
