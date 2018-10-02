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
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
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
	newQuerier := V3ioQuerier{
		container: container,
		mint:      mint, maxt: maxt,
		logger:            logger.GetChild("Querier"),
		cfg:               cfg,
		disableClientAggr: cfg.DisableClientAggr,
	}
	newQuerier.partitionMngr = partMngr
	return &newQuerier
}

type V3ioQuerier struct {
	logger            logger.Logger
	container         *v3io.Container
	cfg               *config.V3ioConfig
	mint, maxt        int64
	partitionMngr     *partmgr.PartitionManager
	disableClientAggr bool
	disableAllAggr    bool
}

//  Standard Time Series Query, return a set of series which match the condition
func (q *V3ioQuerier) Select(name, functions string, step int64, filter string) (SeriesSet, error) {
	return q.selectQry(name, functions, step, nil, filter)
}

// Prometheus Time Series Query, return a set of series which match the condition
func (q *V3ioQuerier) SelectProm(name, functions string, step int64, filter string, noAggr bool) (SeriesSet, error) {
	q.disableClientAggr = true
	q.disableAllAggr = noAggr
	return q.selectQry(name, functions, step, nil, filter)
}

// Overlapping windows Time Series Query, return a set of series each with a list of aggregated results per window
// e.g. get the last 1hr, 6hr, 24hr stats per metric (specify a 1hr step of 3600*1000, 1,6,24 windows, and max time)
func (q *V3ioQuerier) SelectOverlap(name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {
	sort.Sort(sort.Reverse(sort.IntSlice(windows)))
	return q.selectQry(name, functions, step, windows, filter)
}

// base query function
func (q *V3ioQuerier) selectQry(
	name, functions string, step int64, windows []int, filter string) (set SeriesSet, err error) {

	set = nullSeriesSet{}

	filter = strings.Replace(filter, "__name__", "_name", -1)
	q.logger.DebugWith("Select query", "func", functions, "step", step, "filter", filter, "window", windows)
	err = q.partitionMngr.ReadAndUpdateSchema()

	if err != nil {
		return nullSeriesSet{}, errors.Wrap(err, "failed to read/update schema")
	}

	queryTimer, err := performance.ReporterInstanceFromConfig(q.cfg).GetTimer("QueryTimer")

	if err != nil {
		return nullSeriesSet{}, errors.Wrap(err, "failed to create performance metric [QueryTimer]")
	}

	queryTimer.Time(func() {
		filter = strings.Replace(filter, "__name__", "_name", -1)
		q.logger.DebugWith("Select query", "func", functions, "step", step, "filter", filter, "window", windows)

		parts := q.partitionMngr.PartsForRange(q.mint, q.maxt)
		if len(parts) == 0 {
			return
		}

		if len(parts) == 1 {
			set, err = q.queryNumericPartition(parts[0], name, functions, step, windows, filter)
			return
		}

		sets := make([]SeriesSet, len(parts))
		for i, part := range parts {
			set, err := q.queryNumericPartition(part, name, functions, step, windows, filter)
			if err != nil {
				set = nullSeriesSet{}
				return
			}
			sets[i] = set
		}

		// sort each partition when not using range scan
		if name == "" {
			for i := 0; i < len(sets); i++ {
				// TODO make it a Go routine per part
				sorter, err := NewSetSorter(sets[i])
				if err != nil {
					set = nullSeriesSet{}
					return
				}
				sets[i] = sorter
			}
		}

		set, err = newIterSortMerger(sets)
		return
	})

	return
}

// Query a single partition (with numeric/float values)
func (q *V3ioQuerier) queryNumericPartition(
	partition *partmgr.DBPartition, name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {

	mint, maxt := partition.GetPartitionRange()

	if q.maxt < maxt {
		maxt = q.maxt
	}

	if q.mint > mint {
		mint = q.mint
		if step != 0 && step < (maxt-mint) {
			// temporary Aggregation fix: if mint is not aligned with steps, move it to the next step tick
			mint += (maxt - mint) % step
		}
	}

	newSet := &V3ioSeriesSet{mint: mint, maxt: maxt, partition: partition, logger: q.logger}

	// if there is no aggregation function and the step size is grater than the stored aggregate use Average aggr
	// TODO: in non Prometheus we may want avg aggregator for any step>0
	// in Prom range vectors use seek and it would be inefficient to do avg aggregator
	if functions == "" && step > 0 && step >= partition.RollupTime() && partition.AggrType().HasAverage() {
		functions = "avg"
	}

	// if there are aggregations to be made and its not disabled
	if functions != "" && !q.disableAllAggr {

		// if step isn't passed (e.g. when using the console) - the step is the difference between max
		// and min times (e.g. 5 minutes)
		if step == 0 {
			step = maxt - mint
		}

		if step > partition.RollupTime() && q.disableClientAggr {
			step = partition.RollupTime()
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

		// use aggregates if DB side is possible or client aggr is enabled (Prometheus disable client side)
		newSet.canAggregate = newAggrSeries.CanAggregate(partition.AggrType())
		if newSet.canAggregate || !q.disableClientAggr {
			newSet.aggrSeries = newAggrSeries
			newSet.interval = step
			newSet.aggrIdx = newAggrSeries.NumFunctions() - 1
			newSet.overlapWin = windows
			newSet.noAggrLbl = q.disableClientAggr // dont add "Aggregator" label in Prometheus
		}
	}

	err := newSet.getItems(partition, name, filter, q.container, q.cfg.QryWorkers)

	return newSet, err
}

// return the current metric names
func (q *V3ioQuerier) LabelValues(labelKey string) (result []string, err error) {
	labelValuesTimer, err := performance.ReporterInstanceFromConfig(q.cfg).GetTimer("LabelValuesTimer")
	if err != nil {
		return result, errors.Wrap(err, "failed to obtain timer object for [LabelValuesTimer]")
	}

	labelValuesTimer.Time(func() {
		if labelKey == "__name__" {
			result, err = q.getMetricNames()
		} else {
			result, err = q.getLabelValues(labelKey)
		}
	})
	return
}

func (q *V3ioQuerier) Close() error {
	return nil
}

func (q *V3ioQuerier) getMetricNames() ([]string, error) {
	input := v3io.GetItemsInput{
		Path:           q.cfg.TablePath + "/names/",
		AttributeNames: []string{"__name"},
	}

	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, []string{}, q.logger)
	if err != nil {
		return nil, err
	}

	var metricNames []string

	for iter.Next() {
		metricNames = append(metricNames, iter.GetField("__name").(string))
	}

	if iter.Err() != nil {
		q.logger.InfoWith("Failed to read metric names, returning empty list", "err", iter.Err().Error())
	}

	return metricNames, nil
}

func (q *V3ioQuerier) getLabelValues(labelKey string) ([]string, error) {

	// sync partition manager (hack)
	err := q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nil, err
	}

	partitionPaths := q.partitionMngr.GetPartitionsPaths()

	// if no partitions yet - there are no labels
	if len(partitionPaths) == 0 {
		return nil, nil
	}

	labelValuesMap := map[string]struct{}{}

	// get all labelsets
	input := v3io.GetItemsInput{
		Path:           partitionPaths[0],
		AttributeNames: []string{"_lset"},
	}

	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, []string{}, q.logger)
	if err != nil {
		return nil, err
	}

	// iterate over the results
	for iter.Next() {
		labelSet := iter.GetField("_lset").(string)

		// the labelSet will be k1=v1,k2=v2, k2=v3. Assuming labelKey is "k2", we want to convert
		// that to [v2, v3]

		// split at "," to get k=v pairs
		for _, label := range strings.Split(labelSet, ",") {

			// split at "=" to get label key and label value
			splitLabel := strings.SplitN(label, "=", 2)

			// if we got two elements and the first element (the key) is equal to what we're looking
			// for, save the label value in the map. use a map to prevent duplications
			if len(splitLabel) == 2 && splitLabel[0] == labelKey {
				labelValuesMap[splitLabel[1]] = struct{}{}
			}
		}
	}

	if iter.Err() != nil {
		q.logger.InfoWith("Failed to read label values, returning empty list", "err", iter.Err().Error())
	}

	var labelValues []string
	for labelValue := range labelValuesMap {
		labelValues = append(labelValues, labelValue)
	}

	return labelValues, nil
}
