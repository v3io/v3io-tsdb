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
	"bytes"
)

const defaultDbName = "db0"
const defaultContainerId = "bigdata"
const defaultStartTime = 24 * time.Hour

var startTime = (time.Now().UnixNano() - defaultStartTime.Nanoseconds()) / int64(time.Millisecond)
var count = 0 // count real number of samples to compare with query result

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

func runTest(i int, tc *nutest.TestContext, b *testing.B) {
	const sampleStepSize = 1000 // post metrics with one second interval
	sampleTimeMs := startTime + int64(i)*sampleStepSize

	namesCount := 26
	namesDiversity := 10
	lablesCount := 10
	lablesDiversity := 10
	lableValuesCount := 1
	lablesValueDiversity := 1

	samples := makeSamples(
		namesCount, namesDiversity, lablesCount, lablesDiversity, lableValuesCount, lablesValueDiversity, sampleTimeMs)

	for _, sample := range *samples {
		tc.Logger.Debug("Sample data: %s", sample)

		testEvent := nutest.TestEvent{
			Body: []byte(sample),
		}

		resp, err := tc.Invoke(&testEvent)

		if err != nil {
			b.Fatalf("Request has failed!\nError: %s\nResponse: %s\n", err, resp)
		}
		count ++
	}
}

func makeSamples(namesCount, namesDiversity, lablesCount, lableDiversity, lableValueCount,
lableValueDiversity int, timeStamp int64) *[] string {
	names, err := makeNamesRange("Name", namesCount, 1, namesDiversity)
	if err != nil {
		panic(err)
	}
	labels, err := makeNamesRange("Label", lablesCount, 1, lableDiversity)
	if err != nil {
		panic(err)
	}

	sizeEstimate := namesCount * namesDiversity * lablesCount * lableDiversity
	model := make(map[string]map[string][]string, sizeEstimate) // names -> labels -> label values

	for _, name := range names {
		model[name] = make(map[string][]string)
		for _, label := range labels {
			model[name][label] = []string{}
			labelValues, err := makeNamesRange("", lableValueCount, 1, lableValueDiversity)
			if err != nil {
				panic(err)
			}
			for _, labelValues := range labelValues {
				model[name][label] = append(model[name][label], labelValues)
			}
		}
	}

	samples := makeSampleTemplates(model)

	for i, sample := range *samples {
		rand.Seed(time.Now().UnixNano())
		(*samples)[i] = fmt.Sprintf(sample, timeStamp, rand.Float64()*100)
	}

	return samples
}

func makeSampleTemplates(model map[string]map[string][]string) *[]string {
	var result = make([]string, 0)
	for name, labels := range model {
		valSetLenght := 0
		// TODO: find better way to get size of the label values array
		for _, labelValues := range labels {
			valSetLenght = len(labelValues)
			break
		}
		for index := 0; index < valSetLenght; index++ {
			var buffer bytes.Buffer
			buffer.WriteString(fmt.Sprintf("{\"Lset\": { \"__name__\":\"%s\"", name))
			for label, labelValues := range labels {
				buffer.WriteString(", \"")
				buffer.WriteString(label)
				buffer.WriteString(fmt.Sprintf("\" : \"%s\"", labelValues[index]))
			}
			buffer.WriteString("}, \"Time\" : %d, \"Value\" : %f}")
			result = append(result, buffer.String())
		}
	}

	return &result
}

func makeNamesRange(prefix string, count, minIndex, maxIndex int) ([]string, error) {
	if count < 1 {
		return nil, fmt.Errorf("makeNamesRange(%s): Minimal count is 1 but actual is %d", prefix, count)
	}

	if count > 26 {
		// Maximal range contains 26 elements, i.e. <prefix>_[A to Z]
		return nil, fmt.Errorf("makeNamesRange(%s): Maximal number of names is 26 [A..Z], but actual number is %d", prefix, count)
	}

	normalizedPrefix := prefix
	if len(prefix) > 0 {
		normalizedPrefix = fmt.Sprintf("%s_", prefix)
	}

	limit := maxIndex - minIndex + 1
	size := 1
	if count == 1 {
		size = limit
	} else {
		size = count * limit
	}
	array := make([]string, size)

	for i := range array {
		if limit > 1 {
			array[i] = fmt.Sprintf("%s%s_%d", normalizedPrefix, string(65+(i/limit)%count), minIndex+i%limit)
		} else {
			array[i] = fmt.Sprintf("%s%s", normalizedPrefix, string(65+(i/limit)%count))
		}
	}
	return array, nil
}
