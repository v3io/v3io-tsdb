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
	newQuerier.performanceReporter = performance.ReporterInstanceFromConfig(cfg)
	return &newQuerier
}

type V3ioQuerier struct {
	logger              logger.Logger
	container           *v3io.Container
	cfg                 *config.V3ioConfig
	mint, maxt          int64
	partitionMngr       *partmgr.PartitionManager
	disableClientAggr   bool
	disableAllAggr      bool
	performanceReporter *performance.MetricReporter
}

// Standard Time Series Query, return a set of series which match the condition
func (q *V3ioQuerier) Select(name, functions string, step int64, filter string) (SeriesSet, error) {
	return q.selectQry(name, functions, step, nil, filter)
}

// Prometheus time-series query - return a set of time series that match the
// specified conditions
func (q *V3ioQuerier) SelectProm(name, functions string, step int64, filter string, noAggr bool) (SeriesSet, error) {
	q.disableClientAggr = true
	q.disableAllAggr = noAggr
	return q.selectQry(name, functions, step, nil, filter)
}

// Overlapping windows time-series query - return a set of series each with a
// list of aggregated results per window
// For example, get the last 1h, 6h, and 24h stats per metric (specify a 1h
// aggregation interval (step) of 3600*1000 (=1h), windows 1, 6, and 24, and an
// end (max) time).
func (q *V3ioQuerier) SelectOverlap(name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {
	sort.Sort(sort.Reverse(sort.IntSlice(windows)))
	return q.selectQry(name, functions, step, windows, filter)
}

// Base query function
func (q *V3ioQuerier) selectQry(
	name, functions string, step int64, windows []int, filter string) (set SeriesSet, err error) {

	set = nullSeriesSet{}

	filter = strings.Replace(filter, "__name__", "_name", -1)
	q.logger.DebugWith("Select query", "metric", name, "func", functions, "step", step, "filter", filter, "disableAllAggr", q.disableAllAggr, "disableClientAggr", q.disableClientAggr, "window", windows)
	err = q.partitionMngr.ReadAndUpdateSchema()

	if err != nil {
		return nullSeriesSet{}, errors.Wrap(err, "Failed to read/update the TSDB schema.")
	}

	q.performanceReporter.WithTimer("QueryTimer", func() {
		filter = strings.Replace(filter, "__name__", "_name", -1)

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
			set, err = q.queryNumericPartition(part, name, functions, step, windows, filter)
			if err != nil {
				set = nullSeriesSet{}
				return
			}
			sets[i] = set
		}

		// Sort each partition
		/* TODO: Removed condition that applies sorting only on non range scan queries to fix bug with series coming OOO when querying multi partitions,
		Need to think of a better solution.
		*/
		for i := 0; i < len(sets); i++ {
			// TODO make it a Go routine per part
			sorter, error := NewSetSorter(sets[i])
			if error != nil {
				set = nullSeriesSet{}
				err = error
				return
			}
			sets[i] = sorter
		}

		set, err = newIterSortMerger(sets)
		return
	})

	return
}

// Query a single partition (with integer or float values)
func (q *V3ioQuerier) queryNumericPartition(
	partition *partmgr.DBPartition, name, functions string, step int64, windows []int, filter string) (SeriesSet, error) {

	mint, maxt := partition.GetPartitionRange()

	if q.maxt < maxt {
		maxt = q.maxt
	}

	if q.mint > mint {
		mint = q.mint
		if step != 0 && step < (maxt-mint) {
			// Temporary aggregation fix: if mint isn't aligned with the step,
			// move it to the next step tick
			mint += (maxt - mint) % step
		}
	}

	newSet := &V3ioSeriesSet{mint: mint, maxt: maxt, partition: partition, logger: q.logger}

	// If there are no aggregation functions and the aggregation-interval (step)
	// size is greater than the stored aggregate, use the Average aggregate.
	// TODO: When not using the Prometheus TSDB, we may want an avg aggregate
	// for any step>0 in the Prometheus range vectors using seek, and it would
	// be inefficient to use an avg aggregate.
	if functions == "" && step > 0 && step >= partition.RollupTime() && partition.AggrType().HasAverage() {
		functions = "avg"
	}

	// Check whether there are aggregations to add and aggregates aren't disabled
	if functions != "" && !q.disableAllAggr {

		// If step isn't passed (e.g., when using the console), the step is the
		// difference between the end (maxt) and start (mint) times (e.g., 5 minutes)
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

		// Use aggregates if possible on the TSDB side or if client aggregation
		// is enabled (Prometheus is disabled on the client side)
		newSet.canAggregate = newAggrSeries.CanAggregate(partition.AggrType())
		if newSet.canAggregate || !q.disableClientAggr {
			newSet.aggrSeries = newAggrSeries
			newSet.interval = step
			newSet.aggrIdx = newAggrSeries.NumFunctions() - 1
			newSet.overlapWin = windows
			newSet.noAggrLbl = q.disableClientAggr // Don't add an "Aggregate" label in Prometheus (see aggregate.AggregateLabel)
		}
	}

	err := newSet.getItems(partition, name, filter, q.container, q.cfg.QryWorkers)

	return newSet, err
}

// Return the current metric names
func (q *V3ioQuerier) LabelValues(labelKey string) (result []string, err error) {
	q.performanceReporter.WithTimer("LabelValuesTimer", func() {
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
		q.logger.InfoWith("Failed to read metric names; returning an empty list.", "err", iter.Err().Error())
	}

	return metricNames, nil
}

func (q *V3ioQuerier) getLabelValues(labelKey string) ([]string, error) {

	// Sync the partition manager (hack)
	err := q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nil, err
	}

	partitionPaths := q.partitionMngr.GetPartitionsPaths()

	// If there are no partitions yet, there are no labels
	if len(partitionPaths) == 0 {
		return nil, nil
	}

	labelValuesMap := map[string]struct{}{}

	// Get all label sets
	input := v3io.GetItemsInput{
		Path:           partitionPaths[0],
		AttributeNames: []string{"_lset"},
	}

	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, []string{}, q.logger)
	if err != nil {
		return nil, err
	}

	// Iterate over the results
	for iter.Next() {
		labelSet := iter.GetField("_lset").(string)

		// For a label set of k1=v1,k2=v2, k2=v3, for labelKey "k2", for example,
		// we want to convert the set to [v2, v3]

		// Split at "," to get k=v pairs
		for _, label := range strings.Split(labelSet, ",") {

			// Split at "=" to get the label key and label value
			splitLabel := strings.SplitN(label, "=", 2)

			// If we have two elements and the first element (the key) is equal
			// to what we're looking for, save the label value in the map.
			// Use a map to prevent duplicates.
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
