package ingest

import (
	"fmt"
	"testing"
	"os"
	"time"
	"github.com/nuclio/nuclio-test-go"
	"math/rand"
	"log"
	"io/ioutil"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

const DEFAULT_DB_NAME = "db0"
const DEFAULT_CONTAINER_ID = "bigdata"

var osNames = [...]string{"Windows", "Linux", "Unix", "OS X", "iOS", "Android", "Nokia"}
var metricKeys = [...]string{"cpu", "eth", "mem"}
var metricRange map[string][3]int = map[string][3]int{"cpu": {0, 100, 20}, "eth": {0, 10, 10}, "mem": {1, 1024, 0}}

func buildSample(metricName string, os string, node string, time int64, value float64) (sample string, err error) {

	if metricName == "" {
		return "", fmt.Errorf("Metric name should not be empty")
	}

	sampleJsonString := `{
  "Lset": { "__name__":"%s", "os" : "%s", "node" : "%s"},
  "Time" : %d,
  "Value" : %f
}
`

	return fmt.Sprintf(sampleJsonString, metricName, os, node, time, value), nil
}

func BenchmarkIngest(b *testing.B) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	var endpointUrl = os.Getenv("V3IO_URL")
	if endpointUrl == "" {
		endpointUrl = "localhost:8081"
	}

	data := nutest.DataBind{Name: DEFAULT_DB_NAME, Url: endpointUrl, Container: DEFAULT_CONTAINER_ID}
	tc, err := nutest.NewTestContext(Handler, true, &data)
	if err != nil {
		b.Fatal(err)
	}

	err = tc.InitContext(initContext)
	if err != nil {
		b.Fatal(err)
	}

	// run the Fib function b.N times
	for i := 0; i < b.N; i++ {
		runTest(i, tc, b)
	}
}

// InitContext runs only once when the function runtime starts
func initContext(context *nuclio.Context) error {
	cfg, err := config.LoadConfig("")
	if err != nil {
		return err
	}

	data := context.DataBinding[DEFAULT_DB_NAME].(*v3io.Container)
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

func randomInt(min, max int) int {
	rand.Seed(time.Now().UTC().UnixNano())
	return rand.Intn(max-min) + min
}

func runTest(iteration int, tc *nutest.TestContext, b *testing.B) {
	offsetMinutes := -1 * randomInt(0, 24*60)
	sampleTime := time.Now().Add(time.Duration(offsetMinutes) * time.Minute).Unix() * 1000 // x1000 converts seconds to millis
	metricKey := metricKeys[randomInt(0, len(metricKeys))]
	diversity := metricRange[metricKey][2]
	var sampleKey string
	if diversity > 0 {
		sampleKey = fmt.Sprintf("%s_%d", metricKey, randomInt(0, diversity))
	} else {
		sampleKey = metricKey
	}
	sampleValue := rand.Float64() * float64(randomInt(metricRange[metricKey][0], metricRange[metricKey][1]))

	sampleOS := osNames[randomInt(0, len(osNames))]
	sampleDevice := fmt.Sprintf("node_%d", randomInt(0, 2))

	sampleData, _ := buildSample(
		sampleKey,
		sampleOS,
		sampleDevice,
		sampleTime,
		sampleValue)

	tc.Logger.Debug("Run test with time offset of %d minutes. Sample: %s", offsetMinutes, sampleData)

	testEvent := nutest.TestEvent{
		Body: []byte(sampleData),
	}

	resp, err := tc.Invoke(&testEvent)

	if err != nil {
		b.Fatalf("Request has failed!\nError: %s\nResponse: %s\n", err, resp)
	}
}
