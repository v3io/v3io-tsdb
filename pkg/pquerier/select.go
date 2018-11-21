package pquerier

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"strings"
	"sync"
)

const columnWildcard = "*"
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
	isAllColumns        bool
	totalColumns        int

	disableAllAggr    bool
	disableClientAggr bool

	dataFrames      map[uint64]*dataFrame
	frameList       []*dataFrame
	requestChannels []chan *qryResults
	wg              sync.WaitGroup

	timeColumn Column
}

func (s *selectQueryContext) start(parts []*partmgr.DBPartition, params *SelectParams) (*frameIterator, error) {
	s.dataFrames = make(map[uint64]*dataFrame)

	// If step isn't passed (e.g., when using the console), the step is the
	// difference between the end (maxt) and start (mint) times (e.g., 5 minutes)
	if params.Functions != "" && s.step == 0 {
		s.step = s.maxt - s.mint
	}

	s.functions = params.Functions
	var err error
	s.columnsSpec, s.columnsSpecByMetric, err = s.createColumnSpecs(params)
	if err != nil {
		return nil, err
	}

	// We query every partition for every requested metric
	queries := make([]*partQuery, len(parts)*len(s.columnsSpecByMetric))

	var queryIndex int
	for _, part := range parts {
		currQueries, err := s.queryPartition(part)
		if err != nil {
			return nil, err
		}
		for _, q := range currQueries {
			queries[queryIndex] = q
			queryIndex++
		}
	}

	err = s.startCollectors()
	if err != nil {
		return nil, err
	}

	for _, query := range queries {
		err = s.processQueryResults(query)
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < s.workers; i++ {
		close(s.requestChannels[i])
	}

	// wait for Go routines to complete
	s.wg.Wait()

	if len(s.frameList) > 0 {
		s.totalColumns = s.frameList[0].Len()
	}

	frameIter := NewFrameIterator(s)
	return frameIter, nil
}

func (s *selectQueryContext) metricsAggregatesToString(metric string) (string, bool) {
	var result strings.Builder
	specs := s.columnsSpecByMetric[metric]
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
func (s *selectQueryContext) queryPartition(partition *partmgr.DBPartition) ([]*partQuery, error) {
	var queries []*partQuery
	var err error

	mint, maxt := partition.GetPartitionRange()
	step := s.step

	if s.maxt < maxt {
		maxt = s.maxt
	}

	if s.mint > mint {
		mint = s.mint
	}

	for metric := range s.columnsSpecByMetric {
		var aggregationParams *aggregate.AggregationParams
		functions, requestAggregatesAndRaw := s.metricsAggregatesToString(metric)

		//if functions == "" && step > 0 && step >= partition.RollupTime() && partition.AggrType().HasAverage() {
		//	functions = "avg"
		//}

		// Check whether there are aggregations to add and aggregates aren't disabled
		if functions != "" && !s.disableAllAggr {

			if step > partition.RollupTime() && s.disableClientAggr {
				step = partition.RollupTime()
			}

			params, err := aggregate.NewAggregationParams(functions,
				"v",
				partition.AggrBuckets(),
				step,
				partition.RollupTime(),
				s.windows)

			if err != nil {
				return nil, err
			}
			aggregationParams = params

		}

		newQuery := &partQuery{mint: mint, maxt: maxt, partition: partition, step: step}
		if aggregationParams != nil {
			newQuery.preAggregated = aggregationParams.CanAggregate(partition.AggrType())
			if newQuery.preAggregated || !s.disableClientAggr {
				newQuery.aggregationParams = aggregationParams
			}
		}

		err = newQuery.getItems(s, metric, requestAggregatesAndRaw)
		queries = append(queries, newQuery)
	}

	return queries, err
}

func (s *selectQueryContext) startCollectors() error {

	s.requestChannels = make([]chan *qryResults, s.workers)

	for i := 0; i < s.workers; i++ {
		newChan := make(chan *qryResults, 1000)
		s.requestChannels[i] = newChan

		// Increment the WaitGroup counter.
		s.wg.Add(1)
		go func(index int) {
			mainCollector(s, index)
		}(i)
	}

	return nil
}

func (s *selectQueryContext) processQueryResults(query *partQuery) error {
	for query.Next() {

		// read metric name
		name, nok := query.GetField("_name").(string)
		if !nok {
			name = "UNKNOWN"
		}

		// read label set
		lsetAttr, lok := query.GetField(config.LabelSetAttrName).(string)
		if !lok {
			lsetAttr = "UNKNOWN"
		}
		if !lok || !nok {
			s.logger.Error("Error in initLabels; bad field values.")
		}

		splitLset := strings.Split(lsetAttr, ",")
		lset := make(utils.Labels, 0, len(splitLset))
		for _, label := range splitLset {
			kv := strings.Split(label, "=")
			if len(kv) > 1 {
				lset = append(lset, utils.Label{Name: kv[0], Value: kv[1]})
			}
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
		frame, ok := s.dataFrames[hash]
		if !ok {
			var err error
			frame, err = NewDataFrame(s.columnsSpec, s.getOrCreateTimeColumn(), lset, hash, s.isRawQuery(), s.isAllColumns, s.getResultBucketsSize(), results.IsServerAggregates())
			if err != nil {
				return err
			}
			s.dataFrames[hash] = frame
			s.frameList = append(s.frameList, frame)
		}

		results.frame = frame

		workerNum := hash & uint64(s.workers-1)
		s.requestChannels[workerNum] <- &results
	}

	return query.Err()
}

func (s *selectQueryContext) createColumnSpecs(params *SelectParams) ([]columnMeta, map[string][]columnMeta, error) {
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
			tolerance = s.step * defaultToleranceFactor
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
		s.isAllColumns = s.isAllColumns || col.Metric == columnWildcard
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

func (s *selectQueryContext) AddColumnSpecByWildcard(metricName string) {
	_, ok := s.columnsSpecByMetric[metricName]
	if !ok {
		wantedColumns := s.columnsSpecByMetric[columnWildcard]
		newCols := make([]columnMeta, len(wantedColumns))
		for _, col := range wantedColumns {
			newCol := col
			newCol.metric = metricName
			newCols = append(newCols, newCol)
		}
		s.columnsSpec = append(s.columnsSpec, newCols...)
		s.columnsSpecByMetric[metricName] = newCols
	}
}

func (s *selectQueryContext) getOrCreateTimeColumn() Column {
	// When querying for raw data we don't need to generate a time column since we return the raw time
	if s.isRawQuery() {
		return nil
	}
	if s.timeColumn == nil {
		s.timeColumn = s.generateTimeColumn()
	}

	return s.timeColumn
}

func (s *selectQueryContext) generateTimeColumn() Column {
	columnMeta := columnMeta{metric: "time"}
	timeColumn := NewDataColumn("time", columnMeta, s.getResultBucketsSize(), IntType)
	i := 0
	for t := s.mint; t <= s.maxt; t += s.step {
		timeColumn.SetDataAt(i, t)
		i++
	}
	return timeColumn
}

func (s *selectQueryContext) isRawQuery() bool { return s.functions == "" && s.step == 0 }

func (s *selectQueryContext) getResultBucketsSize() int {
	if s.isRawQuery() {
		return 0
	}
	return int((s.maxt-s.mint)/s.step + 1)
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

func (s *partQuery) getItems(ctx *selectQueryContext, name string, aggregatesAndChunk bool) error {

	path := s.partition.GetTablePath()
	var shardingKeys []string
	if name != "" {
		shardingKeys = s.partition.GetShardingKeys(name)
	}
	attrs := []string{config.LabelSetAttrName, config.EncodingAttrName, "_name", config.MaxTimeAttrName}

	if s.preAggregated {
		s.attrs = s.aggregationParams.GetAttrNames()
	}
	// It is possible to request both server aggregates and raw chunk data (to downsample) for the same metric
	// example: `select max(cpu), avg(cpu), cpu` with step = 1h
	if !s.preAggregated || aggregatesAndChunk {
		chunkAttr, chunk0Time := s.partition.Range2Attrs("v", s.mint, s.maxt)
		s.chunk0Time = chunk0Time
		s.attrs = append(s.attrs, chunkAttr...)
	}
	attrs = append(attrs, s.attrs...)

	ctx.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", ctx.filter, "name", name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: ctx.filter, ShardingKey: name}
	iter, err := utils.NewAsyncItemsCursor(ctx.container, &input, ctx.workers, shardingKeys, ctx.logger)
	if err != nil {
		return err
	}

	s.iter = iter
	return nil
}

func (s *partQuery) Next() bool {
	var res bool

	res = s.iter.Next()
	return res
}

func (s *partQuery) GetField(name string) interface{} {
	return s.iter.GetField(name)
}

func (s *partQuery) GetFields() map[string]interface{} {
	return s.iter.GetFields()
}

func (s *partQuery) Err() error {
	return s.iter.Err()
}
