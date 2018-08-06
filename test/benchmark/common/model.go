package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"math/rand"
	"time"
)

func MakeSamplesModel(namesCount, namesDiversity, labelsCount, labelDiversity, labelValueCount,
	labelValueDiversity int) map[string]map[string][]string {
	names, err := MakeNamesRange("Name", namesCount, 1, namesDiversity)
	if err != nil {
		panic(err)
	}
	labels, err := MakeNamesRange("Label", labelsCount, 1, labelDiversity)
	if err != nil {
		panic(err)
	}

	sizeEstimate := namesCount * namesDiversity * labelsCount * labelDiversity
	model := make(map[string]map[string][]string, sizeEstimate) // names -> labels -> label values

	for _, name := range names {
		model[name] = make(map[string][]string)
		for _, label := range labels {
			model[name][label] = []string{}
			labelValues, err := MakeNamesRange("", labelValueCount, 1, labelValueDiversity)
			if err != nil {
				panic(err)
			}
			for _, labelValues := range labelValues {
				model[name][label] = append(model[name][label], labelValues)
			}
		}
	}
	return model
}

func MakeSampleTemplates(model map[string]map[string][]string) []string {
	var result = make([]string, 0)
	for name, labels := range model {
		valSetLength := 0
		// TODO: find better way to get size of the label values array
		for _, labelValues := range labels {
			valSetLength = len(labelValues)
			break
		}
		for index := 0; index < valSetLength; index++ {
			var buffer bytes.Buffer
			buffer.WriteString(fmt.Sprintf(`{"Lset": { "__name__":"%s"`, name))
			for label, labelValues := range labels {
				buffer.WriteString(`, "`)
				buffer.WriteString(label)
				buffer.WriteString(fmt.Sprintf(`" : "%s"`, labelValues[index]))
			}
			buffer.WriteString(`}, "Time" : "%d", "Value" : %.2f}`)
			result = append(result, buffer.String())
		}
	}

	return result
}

func MakeNamesRange(prefix string, count, minIndex, maxIndex int) ([]string, error) {
	if count < 1 {
		return nil, fmt.Errorf("MakeNamesRange(%s): Minimal count is 1 but actual is %d", prefix, count)
	}

	if count > 26 {
		// Maximal range contains 26 elements, i.e. <prefix>_[A to Z]
		return nil, fmt.Errorf("MakeNamesRange(%s): Maximal number of names is 26 [A..Z], but actual number is %d", prefix, count)
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
	slice := make([]string, size)

	for i := range slice {
		if limit > 1 {
			slice[i] = fmt.Sprintf("%s%s_%d", normalizedPrefix, string(65+(i/limit)%count), minIndex+i%limit)
		} else {
			slice[i] = fmt.Sprintf("%s%s", normalizedPrefix, string(65+(i/limit)%count))
		}
	}
	return slice, nil
}

func MakeRandomFloat64() float64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Float64() * 100
}

func JsonTemplate2Sample(sampleJsonTemplate string, time int64, value float64) (*tsdbtest.Sample, error) {
	sampleJson := fmt.Sprintf(sampleJsonTemplate, time, value)
	sample, err := unmarshallSample(sampleJson)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Failed to unmarshall sample: %s", sampleJson))
	}

	return sample, nil
}

func unmarshallSample(sampleJsonString string) (*tsdbtest.Sample, error) {
	sample := tsdbtest.Sample{}
	err := json.Unmarshal([]byte(sampleJsonString), &sample)
	if err != nil {
		return nil, err
	}

	return &sample, nil
}
