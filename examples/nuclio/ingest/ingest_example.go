package main

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/nuclio/nuclio-sdk-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

/*
Example event:
{
		"metric": "cpu",
		"labels": {
			"dc": "7",
			"hostname": "mybesthost"
		},
		"samples": [
			{
				"t": "1532595945142",
				"v": {
					"N": 95.2
				}
			},
			{
				"t": "1532595948517",
				"v": {
					"n": 86.8
				}
			}
		]
}
*/

type value struct {
	N float64 `json:"n,omitempty"`
}

type sample struct {
	Time  string `json:"t"`
	Value value  `json:"v"`
}

type request struct {
	Metric  string            `json:"metric"`
	Labels  map[string]string `json:"labels,omitempty"`
	Samples []sample          `json:"samples"`
}

var tsdbAppender tsdb.Appender
var tsdbAppenderMtx sync.Mutex

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	var request request

	// parse body
	if err := json.Unmarshal(event.GetBody(), &request); err != nil {
		return "", nuclio.WrapErrBadRequest(err)
	}

	if strings.TrimSpace(request.Metric) == "" {
		return nil, nuclio.WrapErrBadRequest(errors.New(`request is missing the mandatory 'metric' field`))
	}

	// convert the map[string]string -> []Labels
	labels := getLabelsFromRequest(request.Metric, request.Labels)

	var ref uint64
	// iterate over request samples
	for _, sample := range request.Samples {

		// if time is not specified assume "now"
		if sample.Time == "" {
			sample.Time = "now"
		}
		// convert time string to time int, string can be: now, now-2h, int (unix milisec time), or RFC3339 date string
		sampleTime, err := utils.Str2unixTime(sample.Time)
		if err != nil {
			return "", errors.Wrap(err, "Failed to parse time: "+sample.Time)
		}
		// append sample to metric
		if ref == 0 {
			ref, err = tsdbAppender.Add(labels, sampleTime, sample.Value.N)
		} else {
			err = tsdbAppender.AddFast(labels, ref, sampleTime, sample.Value.N)
		}
		if err != nil {
			return "", errors.Wrap(err, "Failed to add sample")
		}
	}

	return "", nil
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {
	var err error

	// get configuration from env
	tsdbTablePath := os.Getenv("INGEST_V3IO_TSDB_PATH")
	if tsdbTablePath == "" {
		return errors.New("INGEST_V3IO_TSDB_PATH must be set")
	}

	context.Logger.InfoWith("Initializing", "tsdbTablePath", tsdbTablePath)

	// create TSDB appender
	err = createTSDBAppender(context, tsdbTablePath)
	if err != nil {
		return err
	}

	return nil
}

// convert map[string]string -> utils.Labels
func getLabelsFromRequest(metricName string, labelsFromRequest map[string]string) utils.Labels {

	// adding 1 for metric name
	labels := make(utils.Labels, 0, len(labelsFromRequest)+1)

	// add the metric name
	labels = append(labels, utils.Label{
		Name:  "__name__",
		Value: metricName,
	})

	for labelKey, labelValue := range labelsFromRequest {
		labels = append(labels, utils.Label{
			Name:  labelKey,
			Value: labelValue,
		})
	}

	sort.Sort(labels)

	return labels
}

func createTSDBAppender(context *nuclio.Context, path string) error {
	context.Logger.InfoWith("Creating TSDB appender", "path", path)

	defer tsdbAppenderMtx.Unlock()
	tsdbAppenderMtx.Lock()

	if tsdbAppender == nil {
		v3ioConfig, err := config.GetOrLoadFromStruct(&config.V3ioConfig{
			TablePath: path,
		})
		if err != nil {
			return err
		}
		v3ioUrl := os.Getenv("V3IO_URL")
		numWorkersStr := os.Getenv("V3IO_NUM_WORKERS")
		var numWorkers int
		if len(numWorkersStr) > 0 {
			numWorkers, err = strconv.Atoi(numWorkersStr)
			if err != nil {
				return err
			}
		} else {
			numWorkers = 8
		}
		username := os.Getenv("V3IO_USERNAME")
		if username == "" {
			username = "iguazio"
		}
		password := os.Getenv("V3IO_PASSWORD")
		containerName := os.Getenv("V3IO_CONTAINER")
		if containerName == "" {
			containerName = "bigdata"
		}
		container, err := tsdb.NewContainer(v3ioUrl, numWorkers, "", username, password, containerName, context.Logger)
		if err != nil {
			return err
		}
		// create adapter once for all contexts
		adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, container, context.Logger)
		if err != nil {
			return err
		}
		tsdbAppender, err = adapter.Appender()
		if err != nil {
			return err
		}
	}

	return nil
}
