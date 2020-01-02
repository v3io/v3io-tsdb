// +build unit

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
