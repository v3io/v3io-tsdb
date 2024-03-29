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
package pquerier

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultToleranceFactor = 2

type selectQueryContext struct {
	logger     logger.Logger
	container  v3io.Container
	workers    int
	v3ioConfig *config.V3ioConfig

	queryParams        *SelectParams
	showAggregateLabel bool

	columnsSpec            []columnMeta
	columnsSpecByMetric    map[string][]columnMeta
	totalColumns           int
	isCrossSeriesAggregate bool

	// In case one of the aggregates of one of the metrics should use client side aggregates
	// but the user requested to disable client aggregations - return raw data for every requested metric
	forceRawQuery bool

	dataFrames      map[uint64]*dataFrame
	frameList       []*dataFrame
	requestChannels []chan *qryResults
	errorChannel    chan error
	wg              sync.WaitGroup
	stopChan        chan bool
	finalErrorChan  chan error
}

func (queryCtx *selectQueryContext) start(parts []*partmgr.DBPartition, params *SelectParams) (*frameIterator, error) {
	queryCtx.dataFrames = make(map[uint64]*dataFrame)

	queryCtx.queryParams = params
	var err error
	queryCtx.columnsSpec, queryCtx.columnsSpecByMetric, err = queryCtx.createColumnSpecs()
	if err != nil {
		return nil, err
	}

	// If step isn't passed (e.g., when using the console), the step is the
	// difference between the end (maxt) and start (mint) times (e.g., 5 minutes)
	if queryCtx.hasAtLeastOneFunction() && params.Step == 0 {
		queryCtx.queryParams.Step = params.To - params.From
	}

	// We query every partition for every requested metric
	queries := make([]*partQuery, len(parts)*len(queryCtx.columnsSpecByMetric))

	var queryIndex int
	for _, part := range parts {
		currQueries, err := queryCtx.queryPartition(part)
		if err != nil {
			return nil, err
		}
		for _, q := range currQueries {
			queries[queryIndex] = q
			queryIndex++
		}
	}

	queryCtx.stopChan = make(chan bool, 1)
	queryCtx.finalErrorChan = make(chan error, 1)
	queryCtx.errorChannel = make(chan error, queryCtx.workers+len(queries))

	err = queryCtx.startCollectors()
	if err != nil {
		return nil, err
	}

	for _, query := range queries {
		err = queryCtx.processQueryResults(query)
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < queryCtx.workers; i++ {
		close(queryCtx.requestChannels[i])
	}

	// wait for Go routines to complete
	queryCtx.wg.Wait()
	close(queryCtx.errorChannel)

	// return first error
	err = <-queryCtx.finalErrorChan
	if err != nil {
		return nil, err
	}

	if len(queryCtx.frameList) > 0 {
		queryCtx.totalColumns = queryCtx.frameList[0].Len()
	}

	return newFrameIterator(queryCtx)
}

func (queryCtx *selectQueryContext) metricsAggregatesToString(metric string) (string, bool) {
	var result strings.Builder
	specs := queryCtx.columnsSpecByMetric[metric]
	specsNum := len(specs)
	if specsNum == 0 {
		return "", false
	}

	var requestedRawColumn bool
	result.WriteString(specs[0].function.String())
	for i := 1; i < specsNum; i++ {
		if specs[i].function.String() == "" {
			requestedRawColumn = true
		} else {
			result.WriteString(",")
			result.WriteString(specs[i].function.String())
		}
	}

	return result.String(), requestedRawColumn && result.Len() > 0
}

// Query a single partition
func (queryCtx *selectQueryContext) queryPartition(partition *partmgr.DBPartition) ([]*partQuery, error) {
	var queries []*partQuery
	var err error

	mint, maxt := partition.GetPartitionRange()

	if queryCtx.queryParams.To < maxt {
		maxt = queryCtx.queryParams.To
	}

	if queryCtx.queryParams.From > mint {
		mint = queryCtx.queryParams.From
	}

	queryRawInsteadOfAggregates, doForceAllRawQuery := false, false
	var index int

	for metric := range queryCtx.columnsSpecByMetric {
		var aggregationParams *aggregate.AggregationParams
		functions, requestAggregatesAndRaw := queryCtx.metricsAggregatesToString(metric)

		// Check whether there are aggregations to add and aggregates aren't disabled
		if functions != "" && !queryCtx.queryParams.disableAllAggr {

			if queryCtx.queryParams.Step > partition.RollupTime() && queryCtx.queryParams.disableClientAggr {
				queryCtx.queryParams.Step = partition.RollupTime()
			}

			params, err := aggregate.NewAggregationParams(functions,
				"v",
				partition.AggrBuckets(),
				queryCtx.queryParams.Step,
				queryCtx.queryParams.AggregationWindow,
				partition.RollupTime(),
				queryCtx.queryParams.Windows,
				queryCtx.queryParams.disableClientAggr,
				queryCtx.v3ioConfig.UseServerAggregateCoefficient)

			if err != nil {
				return nil, err
			}
			aggregationParams = params

		}

		newQuery := &partQuery{mint: mint,
			maxt:               maxt,
			partition:          partition,
			step:               queryCtx.queryParams.Step,
			name:               metric,
			aggregatesAndChunk: requestAggregatesAndRaw}
		if aggregationParams != nil {
			// Cross series aggregations cannot use server side aggregates.
			newQuery.useServerSideAggregates = aggregationParams.CanAggregate(partition.AggrType()) &&
				!queryCtx.isCrossSeriesAggregate &&
				!queryCtx.queryParams.UseOnlyClientAggr
			if newQuery.useServerSideAggregates || !queryCtx.queryParams.disableClientAggr {
				newQuery.aggregationParams = aggregationParams
			}
		}

		if newQuery.useServerSideAggregates && !requestAggregatesAndRaw {
			newQuery.preAggregateLabels = queryCtx.parsePreAggregateLabels(partition)
		}

		queries = append(queries, newQuery)

		currentQueryShouldQueryRawInsteadOfAggregates := !newQuery.useServerSideAggregates && queryCtx.queryParams.disableClientAggr
		if len(queryCtx.columnsSpecByMetric) == 1 && currentQueryShouldQueryRawInsteadOfAggregates {
			doForceAllRawQuery = true
		} else if index == 0 {
			queryRawInsteadOfAggregates = currentQueryShouldQueryRawInsteadOfAggregates
		} else if queryRawInsteadOfAggregates != currentQueryShouldQueryRawInsteadOfAggregates {
			doForceAllRawQuery = true
		}
		index++
	}

	if doForceAllRawQuery {
		queryCtx.forceRawQuery = true
		for _, q := range queries {
			q.aggregationParams = nil
			q.useServerSideAggregates = false
			err = q.getItems(queryCtx)
			if err != nil {
				break
			}
		}
	} else {
		for _, q := range queries {
			err = q.getItems(queryCtx)
			if err != nil {
				break
			}
		}
	}

	return queries, err
}

func (queryCtx *selectQueryContext) parsePreAggregateLabels(partition *partmgr.DBPartition) []string {
	if queryCtx.queryParams.GroupBy != "" {
		groupByLabelSlice := strings.Split(queryCtx.queryParams.GroupBy, ",")
		groupByLabelSet := make(map[string]bool)
		for _, groupByLabel := range groupByLabelSlice {
			groupByLabelSet[groupByLabel] = true
		}
	outer:
		for _, preAggr := range partition.PreAggregates() {
			if len(preAggr.Labels) != len(groupByLabelSet) {
				continue
			}
			for _, label := range preAggr.Labels {
				if !groupByLabelSet[label] {
					continue outer
				}
			}
			sort.Strings(groupByLabelSlice)
			return groupByLabelSlice
		}
	}
	return nil
}

func (queryCtx *selectQueryContext) startCollectors() error {

	queryCtx.requestChannels = make([]chan *qryResults, queryCtx.workers)

	// Increment the WaitGroup counter.
	queryCtx.wg.Add(queryCtx.workers)

	for i := 0; i < queryCtx.workers; i++ {
		newChan := make(chan *qryResults, 1000)
		queryCtx.requestChannels[i] = newChan

		go func(index int) {
			mainCollector(queryCtx, queryCtx.requestChannels[index])
		}(i)
	}

	// Watch error channel, and signal all go routines to stop in case of an error
	go func() {
		// Signal all goroutines to stop when error received
		err, ok := <-queryCtx.errorChannel
		if ok && err != nil {
			close(queryCtx.stopChan)
			queryCtx.finalErrorChan <- err
		}

		close(queryCtx.finalErrorChan)
	}()

	return nil
}

func (queryCtx *selectQueryContext) processQueryResults(query *partQuery) error {
	for query.Next() {

		// read metric name
		name, ok := query.GetField(config.MetricNameAttrName).(string)
		if !ok {
			return fmt.Errorf("could not find metric name attribute in response, res:%v", query.GetFields())
		}

		// read label set
		lsetAttr, lok := query.GetField(config.LabelSetAttrName).(string)
		if !lok {
			return fmt.Errorf("could not find label set attribute in response, res:%v", query.GetFields())
		}

		lset, err := utils.LabelsFromString(lsetAttr)
		if err != nil {
			return err
		}

		// read chunk encoding type
		var encoding chunkenc.Encoding
		encodingStr, ok := query.GetField(config.EncodingAttrName).(string)
		// If we don't have the encoding attribute, use XOR as default. (for backwards compatibility)
		if !ok {
			encoding = chunkenc.EncXOR
		} else {
			intEncoding, err := strconv.Atoi(encodingStr)
			if err != nil {
				return fmt.Errorf("error parsing encoding type of chunk, got: %v, error: %v", encodingStr, err)
			}
			encoding = chunkenc.Encoding(intEncoding)
		}

		results := qryResults{name: name, encoding: encoding, query: query, fields: query.GetFields()}
		sort.Sort(lset) // maybe skipped if its written sorted
		var hash uint64

		if queryCtx.queryParams.GroupBy != "" {
			groupByList := strings.Split(queryCtx.queryParams.GroupBy, ",")
			newLset := make(utils.Labels, len(groupByList))
			for i, label := range groupByList {
				trimmed := strings.TrimSpace(label)
				labelValue := lset.Get(trimmed)
				if labelValue != "" {
					newLset[i] = utils.Label{Name: trimmed, Value: labelValue}
				} else {
					return fmt.Errorf("no label named %v found to group by", trimmed)
				}
			}
			lset = newLset
			hash = newLset.Hash()
		} else if queryCtx.isCrossSeriesAggregate {
			hash = uint64(0)
			lset = utils.Labels{}
		} else {
			hash = lset.Hash()
		}

		// find or create data frame
		frame, ok := queryCtx.dataFrames[hash]
		if !ok {
			var err error
			frame, err = newDataFrame(queryCtx.columnsSpec,
				queryCtx.getOrCreateTimeColumn(),
				lset,
				hash,
				queryCtx.isRawQuery(),
				queryCtx.getResultBucketsSize(),
				results.IsServerAggregates(),
				queryCtx.showAggregateLabel)
			if err != nil {
				return err
			}
			queryCtx.dataFrames[hash] = frame
			queryCtx.frameList = append(queryCtx.frameList, frame)
		}

		results.frame = frame
		workerNum := hash & uint64(queryCtx.workers-1)
		queryCtx.requestChannels[workerNum] <- &results
	}

	return query.Err()
}

func (queryCtx *selectQueryContext) createColumnSpecs() ([]columnMeta, map[string][]columnMeta, error) {
	var columnsSpec []columnMeta
	columnsSpecByMetric := make(map[string][]columnMeta)
	requestedColumns, err := queryCtx.queryParams.getRequestedColumns()
	if err != nil {
		return nil, nil, err
	}

	for i, col := range requestedColumns {
		_, ok := columnsSpecByMetric[col.Metric]
		if !ok {
			columnsSpecByMetric[col.Metric] = []columnMeta{}
		}

		inter, err := StrToInterpolateType(col.Interpolator)
		if err != nil {
			return nil, nil, err
		}

		tolerance := col.InterpolationTolerance
		if tolerance == 0 {
			tolerance = queryCtx.queryParams.Step * defaultToleranceFactor
		}
		colMeta := columnMeta{metric: col.Metric, alias: col.Alias, interpolationType: inter, interpolationTolerance: tolerance}

		if col.GetFunction() != "" {
			// validating that all given aggregates are either cross series or not
			if col.isCrossSeries() {
				if i > 0 && !queryCtx.isCrossSeriesAggregate {
					return nil, nil, fmt.Errorf("can not aggregate both over time and across series aggregates")
				}
				queryCtx.isCrossSeriesAggregate = true
			} else if queryCtx.isCrossSeriesAggregate {
				return nil, nil, fmt.Errorf("can not aggregate both over time and across series aggregates")
			}
			aggr, err := aggregate.FromString(col.GetFunction())
			if err != nil {
				return nil, nil, err
			}
			colMeta.function = aggr
		}
		columnsSpecByMetric[col.Metric] = append(columnsSpecByMetric[col.Metric], colMeta)
		columnsSpec = append(columnsSpec, colMeta)
	}

	// Adding hidden columns if needed
	for metric, cols := range columnsSpecByMetric {
		var aggregatesMask aggregate.AggrType
		var aggregates []aggregate.AggrType
		var metricInterpolationType InterpolationType
		var metricInterpolationTolerance int64
		for _, colSpec := range cols {
			aggregatesMask |= colSpec.function
			aggregates = append(aggregates, colSpec.function)

			if metricInterpolationType == 0 {
				if colSpec.interpolationType != 0 {
					metricInterpolationType = colSpec.interpolationType
					metricInterpolationTolerance = colSpec.interpolationTolerance
				}
			} else if colSpec.interpolationType != 0 && colSpec.interpolationType != metricInterpolationType {
				return nil, nil, fmt.Errorf("multiple interpolation for the same metric are not supported, got %v and %v",
					metricInterpolationType.String(),
					colSpec.interpolationType.String())
			} else if metricInterpolationTolerance != colSpec.interpolationTolerance {
				return nil, nil, fmt.Errorf("different interpolation tolerances for the same metric are not supported, got %v and %v",
					metricInterpolationTolerance,
					colSpec.interpolationTolerance)
			}
		}

		// Add hidden aggregates only if there the user specified aggregations
		if aggregatesMask != 0 {
			hiddenColumns := aggregate.GetHiddenAggregatesWithCount(aggregatesMask, aggregates)
			for _, hiddenAggr := range hiddenColumns {
				hiddenCol := columnMeta{metric: metric, function: hiddenAggr, isHidden: true}
				columnsSpec = append(columnsSpec, hiddenCol)
				columnsSpecByMetric[metric] = append(columnsSpecByMetric[metric], hiddenCol)
			}
		}

		// After creating all columns set their interpolation function
		for i := 0; i < len(columnsSpecByMetric[metric]); i++ {
			columnsSpecByMetric[metric][i].interpolationType = metricInterpolationType
			columnsSpecByMetric[metric][i].interpolationTolerance = metricInterpolationTolerance
		}
		for i, col := range columnsSpec {
			if col.metric == metric {
				columnsSpec[i].interpolationType = metricInterpolationType
				columnsSpec[i].interpolationTolerance = metricInterpolationTolerance
			}
		}
	}

	if len(columnsSpec) == 0 {
		return nil, nil, errors.Errorf("no Columns were specified for query: %v", queryCtx.queryParams)
	}
	return columnsSpec, columnsSpecByMetric, nil
}

func (queryCtx *selectQueryContext) getOrCreateTimeColumn() Column {
	// When querying for raw data we don't need to generate a time column since we return the raw time
	if queryCtx.isRawQuery() {
		return nil
	}

	return queryCtx.generateTimeColumn()
}

func (queryCtx *selectQueryContext) generateTimeColumn() Column {
	columnMeta := columnMeta{metric: "time"}
	timeColumn := newDataColumn("time", columnMeta, queryCtx.getResultBucketsSize(), frames.TimeType)
	i := 0
	for t := queryCtx.queryParams.From; t <= queryCtx.queryParams.To; t += queryCtx.queryParams.Step {
		err := timeColumn.SetDataAt(i, time.Unix(t/1000, (t%1000)*1e6))
		if err != nil {
			queryCtx.logger.ErrorWith(errors.Wrap(err, "could not set data"))
		} else {
			i++
		}
	}
	return timeColumn
}

func (queryCtx *selectQueryContext) isRawQuery() bool {
	return (!queryCtx.hasAtLeastOneFunction() && queryCtx.queryParams.Step == 0) ||
		queryCtx.queryParams.disableAllAggr ||
		queryCtx.forceRawQuery
}

func (queryCtx *selectQueryContext) hasAtLeastOneFunction() bool {
	atLeastOneFunction := false
	for _, col := range queryCtx.columnsSpec {
		if col.function != 0 {
			atLeastOneFunction = true
			break
		}
	}
	return atLeastOneFunction
}

func (queryCtx *selectQueryContext) getResultBucketsSize() int {
	if queryCtx.isRawQuery() {
		return 0
	}
	return int((queryCtx.queryParams.To-queryCtx.queryParams.From)/queryCtx.queryParams.Step + 1)
}

// query object for a single partition (or name and partition in future optimizations)

type partQuery struct {
	partition *partmgr.DBPartition
	iter      utils.ItemsCursor
	partIndex int

	baseTime   int64
	mint, maxt int64
	attrs      []string
	step       int64

	chunk0Time              int64
	chunkTime               int64
	useServerSideAggregates bool
	aggregationParams       *aggregate.AggregationParams

	name               string
	preAggregateLabels []string
	aggregatesAndChunk bool
}

func (query *partQuery) getItems(ctx *selectQueryContext) error {

	path := query.partition.GetTablePath()
	if len(query.preAggregateLabels) > 0 {
		path = fmt.Sprintf("%sagg/%s/", path, strings.Join(query.preAggregateLabels, ","))
	}

	var shardingKeys []string
	if query.name != "" {
		shardingKeys = query.partition.GetShardingKeys(query.name)
	}
	attrs := []string{config.LabelSetAttrName, config.EncodingAttrName, config.MetricNameAttrName, config.MaxTimeAttrName, config.ObjectNameAttrName}

	if query.useServerSideAggregates {
		query.attrs = query.aggregationParams.GetAttrNames()
	}
	// It is possible to request both server aggregates and raw chunk data (to downsample) for the same metric
	// example: `select max(cpu), avg(cpu), cpu` with step = 1h
	if !query.useServerSideAggregates || query.aggregatesAndChunk {
		chunkAttr, chunk0Time := query.partition.Range2Attrs("v", query.mint-ctx.queryParams.AggregationWindow, query.maxt)
		query.chunk0Time = chunk0Time
		query.attrs = append(query.attrs, chunkAttr...)
	}
	attrs = append(attrs, query.attrs...)

	ctx.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", ctx.queryParams.Filter, "name", query.name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: ctx.queryParams.Filter, ShardingKey: query.name}
	iter, err := utils.NewAsyncItemsCursor(ctx.container, &input, ctx.workers, shardingKeys, ctx.logger)
	if err != nil {
		return err
	}

	query.iter = iter
	return nil
}

func (query *partQuery) Next() bool {
	return query.iter.Next()
}

func (query *partQuery) GetField(name string) interface{} {
	return query.iter.GetField(name)
}

func (query *partQuery) GetFields() map[string]interface{} {
	return query.iter.GetFields()
}

func (query *partQuery) Err() error {
	return query.iter.Err()
}
