package ingest

import (
	"encoding/json"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb"
	"github.com/v3io/v3io-tsdb/config"
	"time"
)

// Configuration
const tsdbConfig = `
container: "1"
path: "pmetric"
verbose: true 
workers: 32
maxBehind: 5
arraySize: 9000
overrideOld: true
`

// example event
const pushEvent = `
{
  "Lset": { "__name__":"cpu", "os" : "win", "node" : "xyz123"},
  "Time" : 1000,
  "Value" : 3.5
}
`

type Sample struct {
	Lset  labels.Labels
	Time  int64
	Value float64
}

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	sample := Sample{}
	err := json.Unmarshal(event.GetBody(), &sample)
	if err != nil {

	}
	app := context.UserData.(v3io_tsdb.Appender)

	// Add sample to metric, time is specified in Unix * 1000 (milisec)
	_, err = app.Add(sample.Lset, time.Now().Unix()*1000, sample.Value)

	return "", err
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {
	cfg, _ := config.LoadFromData([]byte(tsdbConfig))
	data := context.DataBinding["db0"].(*v3io.Container)
	adapter := v3io_tsdb.NewV3ioAdapter(cfg, data, context.Logger)
	err := adapter.Start()
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
