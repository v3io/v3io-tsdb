package utils

import "github.com/v3io/v3io-tsdb/pkg/utils"

type DataPoint struct {
	Time int64
	Value float64
}

type Sample struct {
	Lset  utils.Labels
	Time  string
	Value float64
}
