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

package querier

import (
	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"strings"
)

// Create a new Querier interface
func NewV3ioQuerier(container *v3io.Container, logger logger.Logger, mint, maxt int64,
	cfg *config.V3ioConfig, partMngr *partmgr.PartitionManager) *V3ioQuerier {
	newQuerier := V3ioQuerier{container: container, mint: mint, maxt: maxt,
		logger: logger.GetChild("Querier"), cfg: cfg}
	newQuerier.partitionMngr = partMngr
	return &newQuerier
}

type V3ioQuerier struct {
	logger        logger.Logger
	container     *v3io.Container
	cfg           *config.V3ioConfig
	mint, maxt    int64
	partitionMngr *partmgr.PartitionManager
}

// Standard Time Series Query, return a set of series which match the condition
func (q *V3ioQuerier) Select(name, functions string, step int64, filter string) (SeriesSet, error) {
	return q.selectQry(name, functions, step, nil, filter)
}

// Overlapping windows Time Series Query, return a set of series each with a list of aggregated results per window
// e.g. get the last 1hr, 6hr, 24hr stats per metric (specify a 1hr step of 3600*1000, 1,6,24 windows, and max time)
func (q *V3ioQuerier) SelectOverlap(name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {
	sort.Sort(sort.Reverse(sort.IntSlice(windows)))
	return q.selectQry(name, functions, step, windows, filter)
}

// base query function
func (q *V3ioQuerier) selectQry(name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {

	filter = strings.Replace(filter, "__name__", "_name", -1)
	q.logger.DebugWith("Select query", "func", functions, "step", step, "filter", filter, "window", windows)
	err := q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nullSeriesSet{}, err
	}
	parts := q.partitionMngr.PartsForRange(q.mint, q.maxt)
	if len(parts) == 0 {
		return nullSeriesSet{}, nil
	}

	if len(parts) == 1 {
		return q.queryNumericPartition(parts[0], name, functions, step, windows, filter)
	}

	sets := make([]SeriesSet, len(parts))
	for i, part := range parts {
		set, err := q.queryNumericPartition(part, name, functions, step, windows, filter)
		if err != nil {
			return nullSeriesSet{}, err
		}
		sets[i] = set
	}

	// sort each partition when not using range scan
	if name == "" {
		for i := 0; i < len(sets); i++ {
			// TODO make it a Go routine per part
			sort, err := NewSetSorter(sets[i])
			if err != nil {
				return nullSeriesSet{}, err
			}
			sets[i] = sort
		}
	}

	return newIterSortMerger(sets)
}

// Query a single partition (with numeric/float values)
func (q *V3ioQuerier) queryNumericPartition(
	partition *partmgr.DBPartition, name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {

	mint, maxt := partition.GetPartitionRange(q.maxt)
	if q.mint > mint {
		mint = q.mint
	}
	if q.maxt < maxt {
		maxt = q.maxt
	}

	newSet := &V3ioSeriesSet{mint: mint, maxt: maxt, partition: partition, logger: q.logger}

	// if there are aggregations to be made
	if functions != "" {

		// if step isn't passed (e.g. when using the console) - the step is the difference between max
		// and min times (e.g. 5 minutes)
		if step == 0 {
			step = maxt - mint
		}

		newAggrSeries, err := aggregate.NewAggregateSeries(functions,
			"v",
			partition.AggrBuckets(),
			step,
			partition.RollupTime(),
			windows)

		if err != nil {
			return nil, err
		}

		newSet.aggrSeries = newAggrSeries
		newSet.interval = step
		newSet.aggrIdx = newAggrSeries.NumFunctions() - 1
		newSet.overlapWin = windows
	}

	err := newSet.getItems(partition, name, filter, q.container, q.cfg.QryWorkers)

	return newSet, err
}

// return the current metric names
func (q *V3ioQuerier) LabelValues(name string) ([]string, error) {

	list := []string{}
	input := v3io.GetItemsInput{Path: q.cfg.Path + "/names/", AttributeNames: []string{"__name"}, Filter: ""}
	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, []string{}, q.logger)
	q.logger.DebugWith("GetItems to read names", "input", input, "err", err)
	if err != nil {
		return list, err
	}

	for iter.Next() {
		name := iter.GetField("__name").(string)
		list = append(list, name)
	}

	if iter.Err() != nil {
		q.logger.InfoWith("Failed to read names, assume empty list", "err", iter.Err().Error())
	}
	return list, nil
}

func (q *V3ioQuerier) Close() error {
	return nil
}
