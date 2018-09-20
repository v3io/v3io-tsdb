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
// Note: the TSDB (path) must be first created using the CLI or API
// the user must also define the v3io data binding in the nuclio function with path, username, password and name it db0
var tsdbConfig = `
path: "pmetric"
`

// example event
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

	// if time is not specified assume "now"
	if sample.Time == "" {
		sample.Time = "now"
	}

	// convert time string to time int, string can be: now, now-2h, int (unix milisec time), or RFC3339 date string
	t, err := utils.Str2unixTime(sample.Time)
	if err != nil {
		return "", err
	}

	// Append sample to metric
	_, err = app.Add(sample.Lset, t, sample.Value)
	return "", err
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {

	var err error
	defer adapterMtx.Unlock()
	adapterMtx.Lock()

	if adapter == nil {
		// create adapter once for all contexts
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
