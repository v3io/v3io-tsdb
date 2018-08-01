package common

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"regexp"
	"strings"
)

func CreateTSDB(v3ioConfig *config.V3ioConfig) error {
	dbcfg := config.DBPartConfig{
		DaysPerObj:     7,
		HrInChunk:      1,
		DefaultRollups: "*",
		RollupMin:      5,
	}
	return tsdb.CreateTSDB(v3ioConfig, &dbcfg)
}

func ValidateCountOfSamples(adapter *tsdb.V3ioAdapter, expected int, startTimeMs, endTimeMs int64) error {
	qry, err := adapter.Querier(nil, startTimeMs, endTimeMs)
	if err != nil {
		return errors.Wrap(err, "failed to create Querier instance.")
	}
	stepSize, err := utils.Str2duration("1h")
	if err != nil {
		return errors.Wrap(err, "failed to create step")
	}
	overlappingWindows := []int{24, 12, 1}
	set, err := qry.SelectOverlap("", "count", stepSize, overlappingWindows, "starts(__name__, 'Name_')")

	var actual int
	for set.Next() {
		if set.Err() != nil {
			return errors.Wrap(set.Err(), "failed to get next element from result set")
		}

		series := set.At()
		iter := series.Iterator()
		for iter.Next() {
			if iter.Err() != nil {
				return errors.Wrap(set.Err(), "failed to get next time-value pair from iterator")
			}

			_, v := iter.At()
			actual += int(v)
		}
	}

	if expected != actual {
		return errors.Errorf("Check failed: actual result is not as expected (%d != %d)", expected, actual)
	} else {
		fmt.Printf("Result is verified. Actual samples count is equal to expected. [%d==%d]\n", expected, actual)
	}

	return nil
}

func NormalizePath(path string) string {
	chars := []string{":", "+"}
	r := strings.Join(chars, "")
	re := regexp.MustCompile("[" + r + "]+")
	return re.ReplaceAllString(path, "_")
}
