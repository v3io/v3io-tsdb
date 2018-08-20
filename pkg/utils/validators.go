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
		return errors.New(fmt.Sprintf("metric name containes illegal characters. Name should conform to '%s'",
			metricNameValidationRegexStr))
	}

	return nil
}

func IsValidLabelName(labelName string) error {
	trimmed := strings.TrimSpace(labelName)
	if len(trimmed) == 0 {
		return errors.New("label name should not be empty")
	}

	if !labelValidationRegex.Match([]byte(trimmed)) {
		return errors.New(fmt.Sprintf("label name containes illegal characters. Label name should conform to '%s'",
			labelValidationRegexStr))
	}

	return nil
}
