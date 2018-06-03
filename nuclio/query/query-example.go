package query

import (
	"github.com/nuclio/nuclio-sdk-go"
	"encoding/json"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"time"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/pkg/errors"
	"strings"
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"bytes"
	"github.com/v3io/v3io-go-http"
)

// Configuration
const tsdbConfig = `
path: "metrpath"
`

type tsdbQuery struct {
	Name          string
	Aggregators   []string
	Step          int64
	Filter        string
	From          int64
	To            int64
}

// example query event
const queryEvent = `
{
  "Name": "http_req"
}
`

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	query := tsdbQuery{}
	err := json.Unmarshal(event.GetBody(), &query)
	if err != nil {
		return nil, err
	}

	if query.To == 0 {
		query.To = time.Now().Unix() * 1000
	}

	if query.From == 0 {
		query.From = query.To - 3600 * 1000   // 1 hour earlier
	}

	// Create TSDB Querier
	context.Logger.DebugWith("Query", "params", query)
	adapter := context.UserData.(*tsdb.V3ioAdapter)
	qry, err := adapter.Querier(nil, query.From, query.To)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Querier")
	}

	// Select Query to get back a series set iterator
	set, err := qry.Select(query.Name, strings.Join(query.Aggregators, ","), query.Step, query.Filter)
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

