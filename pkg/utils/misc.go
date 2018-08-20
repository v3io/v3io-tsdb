package utils

import (
	"fmt"
	"math"
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
