package ingest

import (
	"encoding/json"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"time"
)

// Configuration
const tsdbConfig = `
path: "pmetric"
`

// example event
const pushEvent = `
{
  "Lset": { "__name__":"cpu", "os" : "win", "node" : "xyz123"},
  "Time" : 0,
  "Value" : 3.5
}
`

type Sample struct {
	Lset  utils.Labels
	Time  int64
	Value float64
}

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	sample := Sample{}
	err := json.Unmarshal(event.GetBody(), &sample)
	if err != nil {
		return nil, err
	}
	app := context.UserData.(tsdb.Appender)

	// Add sample to metric, time is specified in Unix * 1000 (milisec)
	t := sample.Time
	if t == 0 {
		t = time.Now().Unix()*1000
	}
	_, err = app.Add(sample.Lset,t , sample.Value)

	return "", err
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {
	cfg, _ := config.LoadFromData([]byte(tsdbConfig))
	data := context.DataBinding["db0"].(*v3io.Container)
	adapter, err := tsdb.NewV3ioAdapter(cfg, data, context.Logger)
	if err != nil {
		return err
	}

	appender, err := adapter.Appender()
	if err != nil {
		return err
	}
	context.UserData = appender
	return nil
}
