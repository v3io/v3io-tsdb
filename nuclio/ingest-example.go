package main

import (
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb"
	"github.com/v3io/v3io-tsdb/config"
)

func InitContext(context *nuclio.Context) error {
	cfg, _ := config.LoadFromData([]byte(""))
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

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	app := context.UserData.(v3io_tsdb.Appender)

	lset := labels.Labels{labels.Label{Name: "__name__", Value: "http_req"},
		labels.Label{Name: "method", Value: "post"}}

	_, err := app.Add(lset, 1000, 2)

	return "", err
}
