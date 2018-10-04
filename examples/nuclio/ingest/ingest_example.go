package ingest

import (
	"encoding/json"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sync"
)

// Configuration
// Note: the TSDB instance (`path`) must first be created using the CLI or API.
// The user must also define the V3IO data binding in the Nuclio function - including path, username, and password - and name it "db0".
var tsdbConfig = `
path: "pmetric"
`

// Example event
const pushEvent = `
{
  "Lset": { "__name__":"cpu", "os" : "win", "node" : "xyz123"},
  "Time" : "now-5m",
  "Value" : 3.7
}
`

var adapter *tsdb.V3ioAdapter
var adapterMtx sync.RWMutex

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	sample := tsdbtest.Sample{}
	err := json.Unmarshal(event.GetBody(), &sample)
	if err != nil {
		return nil, err
	}
	app := context.UserData.(tsdb.Appender)

	// If time isn't specified, assume "now" (default)
	if sample.Time == "" {
		sample.Time = "now"
	}

	// Convert a time string to a Unix timestamp in milliseconds integer.
	// The input time string can be of the format "now", "now-[0-9]+[mdh]"
	// (for example, "now-2h"), "<Unix timestamp in milliseconds>", or
	// "<RFC3339 time>" (for example, "2018-09-26T14:10:20Z").
	t, err := utils.Str2unixTime(sample.Time)
	if err != nil {
		return "", err
	}

	// Append sample to metric
	_, err = app.Add(sample.Lset, t, sample.Value)
	return "", err
}

// InitContext runs only once, when the function runtime starts
func InitContext(context *nuclio.Context) error {

	var err error
	defer adapterMtx.Unlock()
	adapterMtx.Lock()

	if adapter == nil {
		// Create an adapter once for all contexts
		cfg, _ := config.GetOrLoadFromData([]byte(tsdbConfig))
		data := context.DataBinding["db0"].(*v3io.Container)
		adapter, err = tsdb.NewV3ioAdapter(cfg, data, context.Logger)
		if err != nil {
			return err
		}
	}

	appender, err := adapter.Appender()
	if err != nil {
		return err
	}
	context.UserData = appender
	return nil
}
