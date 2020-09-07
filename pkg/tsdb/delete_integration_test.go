// +build integration

package tsdb_test

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

func timeStringToMillis(timeStr string) int64 {
	ta, _ := time.Parse(time.RFC3339, timeStr)
	return ta.Unix() * 1000
}
func TestDeleteTable(t *testing.T) {
	ta, _ := time.Parse(time.RFC3339, "2018-10-03T05:00:00Z")
	t1 := ta.Unix() * 1000
	tb, _ := time.Parse(time.RFC3339, "2018-10-07T05:00:00Z")
	t2 := tb.Unix() * 1000
	tc, _ := time.Parse(time.RFC3339, "2018-10-11T05:00:00Z")
	t3 := tc.Unix() * 1000
	td, _ := time.Parse(time.RFC3339, "2022-10-11T05:00:00Z")
	futurePoint := td.Unix() * 1000

	defaultTimeMillis := timeStringToMillis("2019-07-21T00:00:00Z")
	generalData := []tsdbtest.DataPoint{
		// partition 1
		// chunk a
		{Time: defaultTimeMillis, Value: 1.2},
		{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
		{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
		// chunk b
		{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
		{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

		// partition 2
		// chunk a
		{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
		{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
		{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
		// chunk b
		{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
		{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

		// partition 3
		// chunk a
		{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
		{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
		{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
		// chunk b
		{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
		{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}
	partitions1StartTime := timeStringToMillis("2019-07-21T00:00:00Z")
	partitions2StartTime := timeStringToMillis("2019-07-23T00:00:00Z")
	partitions3StartTime := timeStringToMillis("2019-07-25T00:00:00Z")

	testCases := []struct {
		desc               string
		deleteParams       DeleteParams
		data               tsdbtest.TimeSeries
		expectedData       map[string][]tsdbtest.DataPoint
		expectedPartitions []int64
		ignoreReason       string
	}{
		{desc: "Should delete all table by time",
			deleteParams: DeleteParams{
				From:         0,
				To:           9999999999999,
				IgnoreErrors: true,
			},
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
					{Time: t2, Value: 333.3},
					{Time: t3, Value: 444.4},
					{Time: futurePoint, Value: 555.5}},
			}},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {}},
		},
		{desc: "Should delete all table by deleteAll",
			deleteParams: DeleteParams{
				From:         0,
				To:           t1,
				DeleteAll:    true,
				IgnoreErrors: true,
			},
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
					{Time: t2, Value: 333.3},
					{Time: t3, Value: 444.4},
					{Time: futurePoint, Value: 555.5}},
			}},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {}},
		},
		{desc: "Should delete whole partitions",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime,
				To:   partitions2StartTime - 1,
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole partitions with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:   partitions1StartTime,
				To:     partitions2StartTime - 1,
				Filter: "os == 'win'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-win": {{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-linux": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole partitions specific metrics",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}, tsdbtest.Metric{
				Name: "disk",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime,
				To:      partitions2StartTime - 1,
				Metrics: []string{"cpu"},
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole partitions specific metrics with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "disk",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime,
				To:      partitions2StartTime - 1,
				Metrics: []string{"cpu"},
				Filter:  "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk-linux": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole chunks",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime,
				To:   partitions1StartTime + tsdbtest.HoursInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole chunks with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:   partitions1StartTime,
				To:     partitions1StartTime + tsdbtest.HoursInMillis,
				Filter: "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole chunks specific metrics",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}, tsdbtest.Metric{
				Name: "disk",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime,
				To:      partitions1StartTime + tsdbtest.HoursInMillis,
				Metrics: []string{"cpu"},
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole chunks specific metrics with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "disk",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime,
				To:      partitions1StartTime + tsdbtest.HoursInMillis,
				Metrics: []string{"cpu"},
				Filter:  "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk-linux": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},

		{
			desc: "Should delete partial chunk in the start",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime,
				To:   partitions1StartTime + 4*tsdbtest.MinuteInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{
				"cpu": {
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete partial chunk in the middle",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime + 3*tsdbtest.MinuteInMillis,
				To:   partitions1StartTime + 7*tsdbtest.MinuteInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{
				"cpu": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete partial chunk in the end",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime + 6*tsdbtest.MinuteInMillis,
				To:   partitions1StartTime + 11*tsdbtest.MinuteInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{
				"cpu": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete partial chunk with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:   partitions1StartTime,
				To:     partitions1StartTime + 6*tsdbtest.MinuteInMillis,
				Filter: "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
			},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete partial chunk specific metrics",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}, tsdbtest.Metric{
				Name: "disk",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime,
				To:      partitions1StartTime + 6*tsdbtest.MinuteInMillis,
				Metrics: []string{"cpu"},
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete partial chunk specific metrics with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "disk",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime,
				To:      partitions1StartTime + 6*tsdbtest.MinuteInMillis,
				Metrics: []string{"cpu"},
				Filter:  "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk-linux": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete mixed partitions and chunks",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime + tsdbtest.HoursInMillis,
				To:   partitions3StartTime + 6*tsdbtest.MinuteInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete mixed partitions and chunks with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:   partitions1StartTime + tsdbtest.HoursInMillis,
				To:     partitions3StartTime + 6*tsdbtest.MinuteInMillis,
				Filter: "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
			},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete mixed partitions and chunks specific metrics",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}, tsdbtest.Metric{
				Name: "disk",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime + tsdbtest.HoursInMillis,
				To:      partitions3StartTime + 6*tsdbtest.MinuteInMillis,
				Metrics: []string{"cpu"},
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete mixed partitions and chunks specific metrics with filter",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "win"),
				Data:   generalData,
			}, tsdbtest.Metric{
				Name:   "disk",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   generalData,
			}},
			deleteParams: DeleteParams{
				From:    partitions1StartTime + tsdbtest.HoursInMillis,
				To:      partitions3StartTime + 6*tsdbtest.MinuteInMillis,
				Metrics: []string{"cpu"},
				Filter:  "os == 'linux'",
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu-linux": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"cpu-win": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}},
				"disk-linux": {
					{Time: defaultTimeMillis, Value: 1.2},
					{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
					{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete partially last chunk and update max time",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions3StartTime + 1*tsdbtest.HoursInMillis + 6*tsdbtest.MinuteInMillis,
				To:   partitions3StartTime + 1*tsdbtest.HoursInMillis + 11*tsdbtest.MinuteInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole last chunk and update max time",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions3StartTime + 1*tsdbtest.HoursInMillis,
				To:   partitions3StartTime + 2*tsdbtest.HoursInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
		{
			desc: "Should delete whole all samples in chunk but time range is not bigger then chunk",
			data: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name: "cpu",
				Data: generalData,
			}},
			deleteParams: DeleteParams{
				From: partitions1StartTime + 1*tsdbtest.HoursInMillis + 2*tsdbtest.MinuteInMillis,
				To:   partitions1StartTime + 2*tsdbtest.HoursInMillis + 11*tsdbtest.MinuteInMillis,
			},
			expectedData: map[string][]tsdbtest.DataPoint{"cpu": {
				{Time: defaultTimeMillis, Value: 1.2},
				{Time: defaultTimeMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},

				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 2*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3},

				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.3},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.4},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 5*tsdbtest.MinuteInMillis, Value: 1.2},
				{Time: defaultTimeMillis + 4*tsdbtest.DaysInMillis + 1*tsdbtest.HoursInMillis + 10*tsdbtest.MinuteInMillis, Value: 1.3}}},
			expectedPartitions: []int64{partitions1StartTime, partitions2StartTime, partitions3StartTime},
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testDeleteTSDBCase(t,
				tsdbtest.NewTestParams(t,
					tsdbtest.TestOption{
						Key:   tsdbtest.OptDropTableOnTearDown,
						Value: !test.deleteParams.DeleteAll},
					tsdbtest.TestOption{
						Key:   tsdbtest.OptTimeSeries,
						Value: test.data},
				), test.deleteParams, test.expectedData, test.expectedPartitions)
		})
	}
}

func getCurrentPartitions(test *testing.T, container v3io.Container, path string) []int64 {
	input := &v3io.GetObjectInput{Path: path + "/.schema"}
	resp, err := container.GetObjectSync(input)
	if err != nil {
		test.Fatal(errors.Wrap(err, "failed to get schema"))
	}

	schema := &config.Schema{}
	err = json.Unmarshal(resp.Body(), schema)
	resp.Release()
	if err != nil {
		test.Fatal(errors.Wrapf(err, "Failed to unmarshal schema at path '%s'.", path))
	}

	var partitions []int64
	for _, part := range schema.Partitions {
		partitions = append(partitions, part.StartTime)
	}
	return partitions
}

func testDeleteTSDBCase(test *testing.T, testParams tsdbtest.TestParams, deleteParams DeleteParams,
	expectedData map[string][]tsdbtest.DataPoint, expectedPartitions []int64) {

	adapter, teardown := tsdbtest.SetUpWithData(test, testParams)
	defer teardown()

	container, err := utils.CreateContainer(adapter.GetLogger("container"), testParams.V3ioConfig(), adapter.HTTPTimeout)
	if err != nil {
		test.Fatalf("failed to create new container. reason: %s", err)
	}

	if err := adapter.DeleteDB(deleteParams); err != nil {
		test.Fatalf("Failed to delete DB. reason: %s", err)
	}

	if !deleteParams.DeleteAll {
		actualPartitions := getCurrentPartitions(test, container, testParams.V3ioConfig().TablePath)
		assert.ElementsMatch(test, expectedPartitions, actualPartitions, "remaining partitions are not as expected")

		qry, err := adapter.QuerierV2()
		if err != nil {
			test.Fatalf("Failed to create Querier. reason: %v", err)
		}

		params := &pquerier.SelectParams{
			From:   0,
			To:     math.MaxInt64,
			Filter: "1==1",
		}
		set, err := qry.Select(params)
		if err != nil {
			test.Fatalf("Failed to run Select. reason: %v", err)
		}

		for set.Next() {
			series := set.At()
			labels := series.Labels()
			osLabel := labels.Get("os")
			metricName := labels.Get(config.PrometheusMetricNameAttribute)
			iter := series.Iterator()
			if iter.Err() != nil {
				test.Fatalf("Failed to query data series. reason: %v", iter.Err())
			}

			actual, err := iteratorToSlice(iter)
			if err != nil {
				test.Fatal(err)
			}
			expectedDataKey := metricName
			if osLabel != "" {
				expectedDataKey = fmt.Sprintf("%v-%v", expectedDataKey, osLabel)
			}

			assert.ElementsMatch(test, expectedData[expectedDataKey], actual,
				"result data for '%v' didn't match, expected: %v\n actual: %v\n", expectedDataKey, expectedData[expectedDataKey], actual)

		}
		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}
	} else {
		container, tablePath := adapter.GetContainer()
		tableSchemaPath := path.Join(tablePath, config.SchemaConfigFileName)

		// Validate: schema does not exist
		_, err := container.GetObjectSync(&v3io.GetObjectInput{Path: tableSchemaPath})
		if err != nil {
			if utils.IsNotExistsError(err) {
				// OK - expected
			} else {
				test.Fatalf("Failed to read a TSDB schema from '%s'.\nError: %v", tableSchemaPath, err)
			}
		}

		// Validate: table does not exist
		_, err = container.GetObjectSync(&v3io.GetObjectInput{Path: tablePath})
		if err != nil {
			if utils.IsNotExistsError(err) {
				// OK - expected
			} else {
				test.Fatalf("Failed to read a TSDB schema from '%s'.\nError: %v", tablePath, err)
			}
		}
	}
}
