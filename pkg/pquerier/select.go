package pquerier

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultToleranceFactor = 2

type selectQueryContext struct {
	logger    logger.Logger
	container *v3io.Container
	workers   int

	mint, maxt int64
	step       int64
	filter     string

	// to remove later, replace w column definitions
	functions string
	windows   []int

	// TODO: create columns spec from select query params
	columnsSpec         []columnMeta
	columnsSpecByMetric map[string][]columnMeta
	isAllMetrics        bool
	totalColumns        int

	disableAllAggr     bool
	disableClientAggr  bool
	showAggregateLabel bool

	dataFrames      map[uint64]*dataFrame
	frameList       []*dataFrame
	requestChannels []chan *qryResults
	errorChannel    chan error
	wg              sync.WaitGroup

	timeColumn Column
}

func (queryCtx *selectQueryContext) start(parts []*partmgr.DBPartition, params *SelectParams) (*frameIterator, error) {
	queryCtx.dataFrames = make(map[uint64]*dataFrame)

	// If step isn't passed (e.g., when using the console), the step is the
	// difference between the end (maxt) and start (mint) times (e.g., 5 minutes)
	if params.Functions != "" && queryCtx.step == 0 {
		queryCtx.step = queryCtx.maxt - queryCtx.mint
	}

	queryCtx.functions = params.Functions
	var err error
	queryCtx.columnsSpec, queryCtx.columnsSpecByMetric, err = queryCtx.createColumnSpecs(params)
	if err != nil {
		return nil, err
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

	frameIter := NewFrameIterator(queryCtx)
	return frameIter, nil
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
	step := queryCtx.step

	if queryCtx.maxt < maxt {
		maxt = queryCtx.maxt
	}

	if queryCtx.mint > mint {
		mint = queryCtx.mint
	}

	for metric := range queryCtx.columnsSpecByMetric {
		var aggregationParams *aggregate.AggregationParams
		functions, requestAggregatesAndRaw := queryCtx.metricsAggregatesToString(metric)

		// Check whether there are aggregations to add and aggregates aren't disabled
		if functions != "" && !queryCtx.disableAllAggr {

			if step > partition.RollupTime() && queryCtx.disableClientAggr {
				step = partition.RollupTime()
			}

			params, err := aggregate.NewAggregationParams(functions,
				"v",
				partition.AggrBuckets(),
				step,
				partition.RollupTime(),
				queryCtx.windows)

			if err != nil {
				return nil, err
			}
			aggregationParams = params

		}

		newQuery := &partQuery{mint: mint, maxt: maxt, partition: partition, step: step}
		if aggregationParams != nil {
			newQuery.preAggregated = aggregationParams.CanAggregate(partition.AggrType())
			if newQuery.preAggregated || !queryCtx.disableClientAggr {
				newQuery.aggregationParams = aggregationParams
			}
		}

		err = newQuery.getItems(queryCtx, metric, requestAggregatesAndRaw)
		queries = append(queries, newQuery)
	}

	return queries, err
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

		// read chunk encoding type (TODO: in ingestion etc.)
		encoding, nok := query.GetField(config.EncodingAttrName).(int)
		if !nok {
			encoding = 0
		}

		results := qryResults{name: name, encoding: int16(encoding), query: query, fields: query.GetFields()}
		sort.Sort(lset) // maybe skipped if its written sorted
		hash := lset.Hash()

		// find or create data frame
		frame, ok := queryCtx.dataFrames[hash]
		if !ok {
			var err error
			frame, err = NewDataFrame(queryCtx.columnsSpec, queryCtx.getOrCreateTimeColumn(), lset, hash, queryCtx.isRawQuery(), queryCtx.isAllMetrics, queryCtx.getResultBucketsSize(), results.IsServerAggregates(), queryCtx.showAggregateLabel)
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

func (queryCtx *selectQueryContext) createColumnSpecs(params *SelectParams) ([]columnMeta, map[string][]columnMeta, error) {
	var columnsSpec []columnMeta
	columnsSpecByMetric := make(map[string][]columnMeta)

	for _, col := range params.getRequestedColumns() {
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
			tolerance = queryCtx.step * defaultToleranceFactor
		}
		colMeta := columnMeta{metric: col.Metric, alias: col.Alias, interpolationType: inter, interpolationTolerance: tolerance}

		if col.Function != "" {
			aggr, err := aggregate.AggregateFromString(col.Function)
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
		for _, colSpec := range cols {
			aggregatesMask |= colSpec.function
			aggregates = append(aggregates, colSpec.function)
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
	}

	if len(columnsSpec) == 0 {
		return nil, nil, errors.Errorf("no Columns were specified for query: %v", params)
	}
	return columnsSpec, columnsSpecByMetric, nil
}

func (queryCtx *selectQueryContext) getOrCreateTimeColumn() Column {
	// When querying for raw data we don't need to generate a time column since we return the raw time
	if queryCtx.isRawQuery() {
		return nil
	}
	if queryCtx.timeColumn == nil {
		queryCtx.timeColumn = queryCtx.generateTimeColumn()
	}

	return queryCtx.timeColumn
}

func (queryCtx *selectQueryContext) generateTimeColumn() Column {
	columnMeta := columnMeta{metric: "time"}
	timeColumn := NewDataColumn("time", columnMeta, queryCtx.getResultBucketsSize(), IntType)
	i := 0
	for t := queryCtx.mint; t <= queryCtx.maxt; t += queryCtx.step {
		timeColumn.SetDataAt(i, t)
		i++
	}
	return timeColumn
}

func (queryCtx *selectQueryContext) isRawQuery() bool {
	return (queryCtx.functions == "" && queryCtx.step == 0) || queryCtx.disableClientAggr
}

func (queryCtx *selectQueryContext) getResultBucketsSize() int {
	if queryCtx.isRawQuery() {
		return 0
	}
	return int((queryCtx.maxt-queryCtx.mint)/queryCtx.step + 1)
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

	chunk0Time        int64
	chunkTime         int64
	preAggregated     bool
	aggregationParams *aggregate.AggregationParams
}

func (query *partQuery) getItems(ctx *selectQueryContext, name string, aggregatesAndChunk bool) error {

	path := query.partition.GetTablePath()
	var shardingKeys []string
	if name != "" {
		shardingKeys = query.partition.GetShardingKeys(name)
	}
	attrs := []string{config.LabelSetAttrName, config.EncodingAttrName, config.MetricNameAttrName, config.MaxTimeAttrName}

	if query.preAggregated {
		query.attrs = query.aggregationParams.GetAttrNames()
	}
	// It is possible to request both server aggregates and raw chunk data (to downsample) for the same metric
	// example: `select max(cpu), avg(cpu), cpu` with step = 1h
	if !query.preAggregated || aggregatesAndChunk {
		chunkAttr, chunk0Time := query.partition.Range2Attrs("v", query.mint, query.maxt)
		query.chunk0Time = chunk0Time
		query.attrs = append(query.attrs, chunkAttr...)
	}
	attrs = append(attrs, query.attrs...)

	ctx.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", ctx.filter, "name", name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: ctx.filter, ShardingKey: name}
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
