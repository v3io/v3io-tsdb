package utils

import (
	"fmt"
	"github.com/v3io/v3io-go-http"
	"math"
	"net/http"
	"strings"
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
	errorWithStatusCode, ok := err.(v3io.ErrorWithStatusCode)
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
