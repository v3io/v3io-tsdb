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
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/v3io/v3io-go/pkg/errors"
)

func IsUndefined(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, -1) || math.IsInf(value, 1)
}

func IsDefined(value float64) bool {
	return !IsUndefined(value)
}

func FloatToNormalizedScientificStr(val float64) string {
	if IsUndefined(val) {
		return fmt.Sprintf("%f", val)
	}
	return strings.Replace(fmt.Sprintf("%e", val), "+", "", 1)
}

func IsNotExistsError(err error) bool {
	errorWithStatusCode, ok := err.(v3ioerrors.ErrorWithStatusCode)
	if !ok {
		// error of different type
		return false
	}
	// Ignore 404s
	if errorWithStatusCode.StatusCode() == http.StatusNotFound {
		return true
	}
	return false
}

const (
	errorCodeString              = "ErrorCode"
	falseConditionOuterErrorCode = "16777244"
	falseConditionInnerErrorCode = "16777245"
)

// Check if the current error was caused specifically because the condition was evaluated to false.
func IsFalseConditionError(err error) bool {
	errString := err.Error()

	if strings.Count(errString, errorCodeString) == 2 &&
		strings.Contains(errString, falseConditionOuterErrorCode) &&
		strings.Contains(errString, falseConditionInnerErrorCode) {
		return true
	}

	return false
}

func IsNotExistsOrConflictError(err error) bool {
	errorWithStatusCode, ok := err.(v3ioerrors.ErrorWithStatusCode)
	if !ok {
		// error of different type
		return false
	}
	statusCode := errorWithStatusCode.StatusCode()
	// Ignore 404s and 409s
	if statusCode == http.StatusNotFound || statusCode == http.StatusConflict {
		return true
	}
	return false
}
