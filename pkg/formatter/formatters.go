package formatter

import (
	"io"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"fmt"
	"encoding/csv"
)

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

const metricTemplate = `
  { "target": "%s{%s}",
    "datapoints": [%s]
  }`

func (f simpleJsonFormatter) Write(out io.Writer, set querier.SeriesSet) error {

	firstSeries := true
	output := "["

	for set.Next() {
		if set.Err() != nil {
			return set.Err()
		}

		series := set.At()
		name, labelStr := labelsToStr(series.Labels())
		datapoints := ""

		iter := series.Iterator()
		firstItem := true
		for iter.Next() {

			if iter.Err() != nil {
				return iter.Err()
			}

			t, v := iter.At()
			if !firstItem {
				datapoints = datapoints + ","
			}
			datapoints = datapoints + fmt.Sprintf("[%.6f,%d]", v, t)
			firstItem = false
		}

		if !firstSeries {
			output = output + ","
		}
		output = output + fmt.Sprintf(metricTemplate, name, labelStr, datapoints)
		firstSeries = false
	}

	_, err := out.Write([]byte(output + "\n]"))

	return err
}

