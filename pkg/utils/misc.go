package utils

import (
	"fmt"
	"strings"
)

func FloatToNormalizedScientificStr(val float64) string {
	return strings.Replace(fmt.Sprintf("%e", val), "+", "", 1)
}
