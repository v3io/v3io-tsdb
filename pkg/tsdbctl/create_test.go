// +build unit

package tsdbctl

import "testing"

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
