package formatter

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const DefaultOutputFormat = "text"

func NewFormatter(format string, cfg *FormatterConfig) (Formatter, error) {
	if cfg == nil {
		cfg = &FormatterConfig{TimeFormat: time.RFC3339}
	}
	switch format {
	case "", DefaultOutputFormat:
		return textFormatter{baseFormatter{cfg: cfg}}, nil
	case "csv":
		return csvFormatter{baseFormatter{cfg: cfg}}, nil
	case "json":
		return simpleJsonFormatter{baseFormatter{cfg: cfg}}, nil
	case "none":
		return testFormatter{baseFormatter{cfg: cfg}}, nil

	default:
		return nil, fmt.Errorf("unknown formatter type %s", format)
	}
}

type Formatter interface {
	Write(out io.Writer, set utils.SeriesSet) error
}

type FormatterConfig struct {
	TimeFormat string
}

type baseFormatter struct {
	cfg *FormatterConfig
}

func labelsToStr(labels utils.Labels) (string, string) {
	name := ""
	var lbls []string
	for _, lbl := range labels {
		if lbl.Name == "__name__" {
			name = lbl.Value
		} else {
			lbls = append(lbls, lbl.Name+"="+lbl.Value)
		}
	}
	return name, strings.Join(lbls, ",")
}
