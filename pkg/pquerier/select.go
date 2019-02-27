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
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultToleranceFactor = 2

type selectQueryContext struct {
	logger    logger.Logger
	container *v3io.Container
	workers   int

	queryParams        *SelectParams
	showAggregateLabel bool

	columnsSpec            []columnMeta
	columnsSpecByMetric    map[string][]columnMeta
	isAllMetrics           bool
	totalColumns           int
	isCrossSeriesAggregate bool

	dataFrames      map[uint64]*dataFrame
	frameList       []*dataFrame
	requestChannels []chan *qryResults
	errorChannel    chan error
	wg              sync.WaitGroup
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
	err = <-queryCtx.errorChannel
	if err != nil {
		return nil, err
	}

	if len(queryCtx.frameList) > 0 {
		queryCtx.totalColumns = queryCtx.frameList[0].Len()
	}

	return NewFrameIterator(queryCtx)
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
	step := queryCtx.queryParams.Step

	if queryCtx.queryParams.To < maxt {
		maxt = queryCtx.queryParams.To
	}

	if queryCtx.queryParams.From > mint {
		mint = queryCtx.queryParams.From
	}

	for metric := range queryCtx.columnsSpecByMetric {
		var aggregationParams *aggregate.AggregationParams
		functions, requestAggregatesAndRaw := queryCtx.metricsAggregatesToString(metric)

		// Check whether there are aggregations to add and aggregates aren't disabled
		if functions != "" && !queryCtx.queryParams.disableAllAggr {

			if step > partition.RollupTime() && queryCtx.queryParams.disableClientAggr {
				step = partition.RollupTime()
			}

			params, err := aggregate.NewAggregationParams(functions,
				"v",
				partition.AggrBuckets(),
				step,
				partition.RollupTime(),
				queryCtx.queryParams.Windows)

			if err != nil {
				return nil, err
			}
			aggregationParams = params

		}

		newQuery := &partQuery{mint: mint, maxt: maxt, partition: partition, step: step}
		if aggregationParams != nil {
			// Cross series aggregations cannot use server side aggregates.
			newQuery.useServerSideAggregates = aggregationParams.CanAggregate(partition.AggrType()) && !queryCtx.isCrossSeriesAggregate
			if newQuery.useServerSideAggregates || !queryCtx.queryParams.disableClientAggr {
				newQuery.aggregationParams = aggregationParams
			}
		}

		var preAggregateLabels []string
		if newQuery.useServerSideAggregates && !requestAggregatesAndRaw {
			preAggregateLabels = queryCtx.parsePreAggregateLabels(partition)
		}
		err = newQuery.getItems(queryCtx, metric, preAggregateLabels, requestAggregatesAndRaw)
		queries = append(queries, newQuery)
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
	queryCtx.errorChannel = make(chan error, queryCtx.workers)

	// Increment the WaitGroup counter.
	queryCtx.wg.Add(queryCtx.workers)

	for i := 0; i < queryCtx.workers; i++ {
		newChan := make(chan *qryResults, 1000)
		queryCtx.requestChannels[i] = newChan

		go func(index int) {
			mainCollector(queryCtx, queryCtx.requestChannels[index])
		}(i)
	}

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
			} else {
				encoding = chunkenc.Encoding(intEncoding)
			}
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
			frame, err = NewDataFrame(queryCtx.columnsSpec,
				queryCtx.getOrCreateTimeColumn(),
				lset,
				hash,
				queryCtx.isRawQuery(),
				queryCtx.isAllMetrics,
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
	for i, col := range queryCtx.queryParams.getRequestedColumns() {
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
			aggr, err := aggregate.AggregateFromString(col.GetFunction())
			if err != nil {
				return nil, nil, err
			}
			colMeta.function = aggr
		}
		columnsSpecByMetric[col.Metric] = append(columnsSpecByMetric[col.Metric], colMeta)
		columnsSpec = append(columnsSpec, colMeta)
		queryCtx.isAllMetrics = queryCtx.isAllMetrics || col.Metric == ""
	}

	// Adding hidden columns if needed
	for metric, cols := range columnsSpecByMetric {
		var aggregatesMask aggregate.AggrType
		var aggregates []aggregate.AggrType
		var metricInterpolationType InterpolationType
		for _, colSpec := range cols {
			aggregatesMask |= colSpec.function
			aggregates = append(aggregates, colSpec.function)

			if metricInterpolationType == 0 {
				if colSpec.interpolationType != 0 {
					metricInterpolationType = colSpec.interpolationType
				}
			} else if colSpec.interpolationType != 0 && colSpec.interpolationType != metricInterpolationType {
				return nil, nil, fmt.Errorf("multiple interpolation for the same metric are not supported, got %v and %v",
					metricInterpolationType.String(),
					colSpec.interpolationType.String())
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
		}
		for i, col := range columnsSpec {
			if col.metric == metric {
				columnsSpec[i].interpolationType = metricInterpolationType
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
	timeColumn := NewDataColumn("time", columnMeta, queryCtx.getResultBucketsSize(), frames.TimeType)
	i := 0
	for t := queryCtx.queryParams.From; t <= queryCtx.queryParams.To; t += queryCtx.queryParams.Step {
		timeColumn.SetDataAt(i, time.Unix(t/1000, (t%1000)*1e6))
		i++
	}
	return timeColumn
}

func (queryCtx *selectQueryContext) isRawQuery() bool {
	return (!queryCtx.hasAtLeastOneFunction() && queryCtx.queryParams.Step == 0) || queryCtx.queryParams.disableClientAggr
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
}

func (query *partQuery) getItems(ctx *selectQueryContext, name string, preAggregateLabels []string, aggregatesAndChunk bool) error {

	path := query.partition.GetTablePath()
	if len(preAggregateLabels) > 0 {
		path = fmt.Sprintf("%sagg/%s/", path, strings.Join(preAggregateLabels, ","))
	}

	var shardingKeys []string
	if name != "" {
		shardingKeys = query.partition.GetShardingKeys(name)
	}
	attrs := []string{config.LabelSetAttrName, config.EncodingAttrName, config.MetricNameAttrName, config.MaxTimeAttrName, config.ObjectNameAttrName}

	if query.useServerSideAggregates {
		query.attrs = query.aggregationParams.GetAttrNames()
	}
	// It is possible to request both server aggregates and raw chunk data (to downsample) for the same metric
	// example: `select max(cpu), avg(cpu), cpu` with step = 1h
	if !query.useServerSideAggregates || aggregatesAndChunk {
		chunkAttr, chunk0Time := query.partition.Range2Attrs("v", query.mint, query.maxt)
		query.chunk0Time = chunk0Time
		query.attrs = append(query.attrs, chunkAttr...)
	}
	attrs = append(attrs, query.attrs...)

	ctx.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", ctx.queryParams.Filter, "name", name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: ctx.queryParams.Filter, ShardingKey: name}
	iter, err := utils.NewAsyncItemsCursor(ctx.container, &input, ctx.workers, shardingKeys, ctx.logger)
	if err != nil {
		return err
	}

	query.iter = iter
	return nil
}

func (query *partQuery) Next() bool {
	var res bool

	res = query.iter.Next()
	return res
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
