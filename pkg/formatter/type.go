package formatter

import (
	"io"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"fmt"
	"time"
	"strconv"
	"strings"
	"encoding/csv"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"encoding/json"
	"github.com/pkg/errors"
)

func NewFormatter(format string, cfg *FormatterConfig) (Formatter, error) {
	if cfg == nil {
		cfg = &FormatterConfig{}
	}
	switch format {
	case "", "text":
		return textFormatter{baseFormatter{cfg:cfg}}, nil
	case "csv":
		return csvFormatter{baseFormatter{cfg:cfg}}, nil
	case "json":
		return simpleJsonFormatter{baseFormatter{cfg:cfg}}, nil

	default:
		return nil, fmt.Errorf("unknown formatter type %s", format)
	}
}

type Formatter interface {
	Write(out io.Writer, set querier.SeriesSet) error
}

type FormatterConfig struct {
	TimeFormat    string
}

type baseFormatter struct {
	cfg  *FormatterConfig
}

func (f baseFormatter) timeString(t int64) string {
	if f.cfg.TimeFormat == "" {
		return strconv.Itoa(int(t))
	}

	return time.Unix(t/1000, 0).Format(f.cfg.TimeFormat)
}


type textFormatter struct {
	baseFormatter
}

func (f textFormatter) Write(out io.Writer, set querier.SeriesSet) error {

	for set.Next() {
		if set.Err() != nil {
			return set.Err()
		}

		series := set.At()
		name, lbls := labelsToStr(series.Labels())
		fmt.Fprintf(out, "Name: %s  Labels: %s\n", name, lbls)
		iter := series.Iterator()
		for iter.Next() {

			if iter.Err() != nil {
				return iter.Err()
			}

			t, v := iter.At()
			fmt.Fprintf(out,"  %s  v=%.2f\n", f.timeString(t), v)
		}
		fmt.Fprintln(out,"")
	}

	return nil
}


type csvFormatter struct {
	baseFormatter
}

func (f csvFormatter) Write(out io.Writer, set querier.SeriesSet) error {

	writer := csv.NewWriter(out)
	for set.Next() {
		if set.Err() != nil {
			return set.Err()
		}

		series := set.At()
		name, labelStr := labelsToStr(series.Labels())

		iter := series.Iterator()
		for iter.Next() {

			if iter.Err() != nil {
				return iter.Err()
			}

			t, v := iter.At()
			writer.Write([]string{name, labelStr, fmt.Sprintf("%.6f", v), f.timeString(t)})
		}
	}

	writer.Flush()
	return nil

}

type simpleJsonFormatter struct {
	baseFormatter
}

type simpleJsonRecord struct {
	Target      string         `json:"target"`
	Datapoints  [][2]float64   `json:"datapoints"`
}

func (f simpleJsonFormatter) Write(out io.Writer, set querier.SeriesSet) error {

	records := []simpleJsonRecord{}

	for set.Next() {
		if set.Err() != nil {
			return set.Err()
		}

		series := set.At()
		name, labelStr := labelsToStr(series.Labels())
		datapoints := [][2]float64{}

		iter := series.Iterator()
		for iter.Next() {

			if iter.Err() != nil {
				return iter.Err()
			}

			t, v := iter.At()
			datapoints = append(datapoints, [2]float64{ v, float64(t)})
		}

		records = append(records, simpleJsonRecord{Target: name + ":" + labelStr, Datapoints: datapoints})
	}

	data, err := json.Marshal(records)
	if err != nil {
		return errors.Wrap(err, "cant marshal series")
	}

	_, err = out.Write(data)

	return err
}

func labelsToStr(labels utils.Labels) (string, string) {
	name := ""
	lbls := []string{}
	for _, lbl := range labels {
		if lbl.Name == "__name__" {
			name = lbl.Value
		} else {
			lbls = append(lbls, lbl.Name + "=" + lbl.Value)
		}
	}
	return name, strings.Join(lbls, ",")
}