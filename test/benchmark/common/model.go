/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
)

var lastValue int64 = 0

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
			slice[i] = fmt.Sprintf("%s%s_%d", normalizedPrefix, string(rune(65+(i/limit)%count)), minIndex+i%limit)
		} else {
			slice[i] = fmt.Sprintf("%s%s", normalizedPrefix, string(rune(65+(i/limit)%count)))
		}
	}
	return slice, nil
}

func NextValue(sequential bool) float64 {
	if sequential {
		return float64(atomic.AddInt64(&lastValue, 1))
	} else {
		return MakeRandomFloat64()
	}
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
