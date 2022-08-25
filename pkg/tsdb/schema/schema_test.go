// +build unit

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

package schema

import (
	"fmt"
	"testing"
)

func TestRateToHour(t *testing.T) {
	cases := []struct {
		input      string
		output     int
		shouldFail bool
	}{
		{input: "1/s", output: 3600},
		{input: "12/m", output: 12 * 60},
		{input: "2/h", output: 2},
		{input: "1m", shouldFail: true},
		{input: "1/t", shouldFail: true},
		{input: "-431/t", shouldFail: true},
		{input: "-1", shouldFail: true},
		{input: "", shouldFail: true},
	}

	for _, testCase := range cases {
		t.Run(testCase.input, func(t *testing.T) {
			actual, err := rateToHours(testCase.input)
			if err != nil && !testCase.shouldFail {
				t.Fatalf("got unexpected error %v", err)
			} else if actual != testCase.output {
				t.Fatalf("actual %v is not equal to expected %v", actual, testCase.output)
			}
		})
	}
}

func TestAggregationGranularityValidation(t *testing.T) {
	cases := []struct {
		granularity       string
		partitionInterval string
		hasAggregates     bool
		shouldFail        bool
	}{
		{granularity: "1h", partitionInterval: "48h", hasAggregates: true, shouldFail: false},
		{granularity: "15m", partitionInterval: "2880h", hasAggregates: true, shouldFail: false},
		{granularity: "1h", partitionInterval: "150000h", hasAggregates: true, shouldFail: true},
		{granularity: "1h", partitionInterval: "150000h", hasAggregates: false, shouldFail: false},
		{granularity: "30m", partitionInterval: "75000h", hasAggregates: true, shouldFail: true},
	}

	for _, testCase := range cases {
		testName := fmt.Sprintf("%v - %v - %v",
			testCase.granularity, testCase.partitionInterval, testCase.hasAggregates)
		t.Run(testName, func(t *testing.T) {
			err := validateAggregatesGranularity(testCase.granularity, testCase.partitionInterval, testCase.hasAggregates)

			if err != nil && !testCase.shouldFail ||
				err == nil && testCase.shouldFail {
				t.Fatalf("test shouldFail=%v, and got error: %v", testCase.shouldFail, err)
			}
		})
	}
}
