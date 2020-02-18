package formatter

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type textFormatter struct {
	baseFormatter
}

func (f textFormatter) Write(out io.Writer, set utils.SeriesSet) error {

	for set.Next() {
		series := set.At()
		name, lbls := labelsToStr(series.Labels())
		fmt.Fprintf(out, "Name: %s  Labels: %s\n", name, lbls)
		iter := series.Iterator()
		for iter.Next() {
			if iter.Encoding() == chunkenc.EncXOR {
				t, v := iter.At()
				fmt.Fprintf(out, "  %s  v=%.2f\n", f.timeString(t), v)
			} else {
				t, v := iter.AtString()
				fmt.Fprintf(out, "  %s  v=%v\n", f.timeString(t), v)
			}
		}

		if iter.Err() != nil {
			return iter.Err()
		}

		fmt.Fprintln(out, "")
	}

	if set.Err() != nil {
		return set.Err()
	}

	return nil
}

func (f textFormatter) timeString(t int64) string {
	if f.cfg.TimeFormat == "" {
		return strconv.Itoa(int(t))
	}
	return time.Unix(t/1000, 0).Format(f.cfg.TimeFormat)
}

type csvFormatter struct {
	baseFormatter
}

func (f csvFormatter) Write(out io.Writer, set utils.SeriesSet) error {

	writer := csv.NewWriter(out)
	for set.Next() {

		series := set.At()
		name, labelStr := labelsToStr(series.Labels())

		iter := series.Iterator()
		for iter.Next() {
			if iter.Encoding() == chunkenc.EncXOR {
				t, v := iter.At()
				_ = writer.Write([]string{name, labelStr, fmt.Sprintf("%.6f", v), strconv.FormatInt(t, 10)})
			} else {
				t, v := iter.AtString()
				_ = writer.Write([]string{name, labelStr, fmt.Sprintf("%v", v), strconv.FormatInt(t, 10)})
			}
		}

		if iter.Err() != nil {
			return iter.Err()
		}
	}

	if set.Err() != nil {
		return set.Err()
	}

	writer.Flush()
	return nil

}

type simpleJSONFormatter struct {
	baseFormatter
}

const metricTemplate = `
  { "target": "%s{%s}",
    "datapoints": [%s]
  }`

func (f simpleJSONFormatter) Write(out io.Writer, set utils.SeriesSet) error {

	firstSeries := true
	output := "["

	for set.Next() {
		series := set.At()
		name, labelStr := labelsToStr(series.Labels())
		datapoints := ""

		iter := series.Iterator()
		firstItem := true
		for iter.Next() {

			if !firstItem {
				datapoints = datapoints + ","
			}
			if iter.Encoding() == chunkenc.EncXOR {
				t, v := iter.At()
				datapoints = datapoints + fmt.Sprintf("[%.6f,%d]", v, t)
			} else {
				t, v := iter.AtString()
				datapoints = datapoints + fmt.Sprintf("[\"%v\",%d]", v, t)
			}

			firstItem = false
		}

		if iter.Err() != nil {
			return iter.Err()
		}

		if !firstSeries {
			output = output + ","
		}
		output = output + fmt.Sprintf(metricTemplate, name, labelStr, datapoints)
		firstSeries = false
	}

	if set.Err() != nil {
		return set.Err()
	}

	_, err := out.Write([]byte(output + "\n]"))

	return err
}

type testFormatter struct {
	baseFormatter
}

func (f testFormatter) Write(out io.Writer, set utils.SeriesSet) error {
	var count int
	for set.Next() {
		count++
		series := set.At()
		iter := series.Iterator()
		var i int
		for iter.Next() {
			i++
		}

		if iter.Err() != nil {
			return errors.Errorf("error reading point for label set: %v, at index: %v, error: %v", series.Labels(), i, iter.Err())
		}
	}

	if set.Err() != nil {
		return set.Err()
	}

	fmt.Fprintf(out, "got %v unique label sets\n", count)
	return nil
}
