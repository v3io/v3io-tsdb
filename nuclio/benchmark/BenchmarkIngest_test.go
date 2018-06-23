package benchmark

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
	"github.com/v3io/v3io-tsdb/nuclio/ingest"
)

const defaultDbName = "db0"
const defaultContainerId = "bigdata"
const defaultStartTime = 24 * time.Hour

var startTime = (time.Now().UnixNano() - defaultStartTime.Nanoseconds()) / int64(time.Millisecond)
var count = 0 // count real number of samples to compare with query result
var osNames = [...]string{"Windows", "Linux", "Unix", "OS X", "iOS", "Android", "Nokia"}
var metricKeys = [...]string{"cpu", "eth", "mem"}
var deviceIds = [...]string{"0", "1"}
var metricRange = map[string][3]int{"cpu": {0, 100, 20}, "eth": {0, 1024 * 1024 * 1024, 10}, "mem": {1, 1024, 0}}

func BenchmarkRandomIngest(b *testing.B) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	var endpointUrl = os.Getenv("V3IO_URL")
	if endpointUrl == "" {
		endpointUrl = "localhost:8081"
	}

	data := nutest.DataBind{Name: defaultDbName, Url: endpointUrl, Container: defaultContainerId}
	tc, err := nutest.NewTestContext(ingest.Handler, false, &data)
	if err != nil {
		b.Fatal(err)
	}

	err = tc.InitContext(initContext)
	if err != nil {
		b.Fatal(err)
	}

	// run the runTest function b.N times
	for i := 0; i < b.N; i++ {
		runTest(i, tc, b)
	}

	tc.Logger.Warn("Test complete. Count: %d", count)
}

// InitContext runs only once when the function runtime starts
func initContext(context *nuclio.Context) error {
	cfg, err := config.LoadConfig("")
	if err != nil {
		return err
	}

	data := context.DataBinding[defaultDbName].(*v3io.Container)
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

func runTest(i int, tc *nutest.TestContext, b *testing.B) {
	const sampleStepSize = 5 * 1000 // post metrics with 5 seconds intervals
	sampleTimeMs := startTime + int64(i)*sampleStepSize

	for _, metricKey := range metricKeys {
		diversity := metricRange[metricKey][2]

		var sampleKey = metricKey
		if diversity > 0 {
			sampleKey = fmt.Sprintf("%s_%d", metricKey, randomInt(0, diversity))
		}

		for _, sampleOS := range osNames {
			for _, sampleDeviceId := range deviceIds {
				sampleDevice := fmt.Sprintf("node_%s", sampleDeviceId)
				sampleValue := rand.Float64() * float64(randomInt(metricRange[metricKey][0], metricRange[metricKey][1]))

				sampleJsonString := `{
  "Lset": { "__name__":"%s", "os" : "%s", "node" : "%s"},
  "Time" : %d,
  "Value" : %f
}
`
				sampleData := fmt.Sprintf(sampleJsonString, sampleKey, sampleOS, sampleDevice, sampleTimeMs, sampleValue)
				tc.Logger.Debug("Sample data: %s", sampleData)

				testEvent := nutest.TestEvent{
					Body: []byte(sampleData),
				}

				resp, err := tc.Invoke(&testEvent)

				if err != nil {
					b.Fatalf("Request has failed!\nError: %s\nResponse: %s\n", err, resp)
				}
				count ++
			}
		}
	}
}
