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
package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	metricNameValidationRegexStr = `^[a-zA-Z_:]([a-zA-Z0-9_:])*$`
	labelValidationRegexStr      = `^[a-zA-Z_]([a-zA-Z0-9_])*$`
)

var metricNameValidationRegex = regexp.MustCompile(metricNameValidationRegexStr)
var labelValidationRegex = regexp.MustCompile(labelValidationRegexStr)

func IsValidMetricName(name string) error {
	trimmed := strings.TrimSpace(name)
	if len(trimmed) == 0 {
		return errors.New("metric name should not be empty")
	}

	if !metricNameValidationRegex.Match([]byte(trimmed)) {
		return fmt.Errorf("metric name contains illegal characters. Name '%s' should conform to '%s'",
			trimmed, metricNameValidationRegexStr)
	}

	return nil
}

func IsValidLabelName(labelName string) error {
	trimmed := strings.TrimSpace(labelName)
	if len(trimmed) == 0 {
		return errors.New("label name should not be empty")
	}

	if !labelValidationRegex.Match([]byte(trimmed)) {
		return fmt.Errorf("label name contains illegal characters. Label name '%s' should conform to '%s'",
			trimmed, labelValidationRegexStr)
	}

	return nil
}
