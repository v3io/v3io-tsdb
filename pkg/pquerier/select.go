package pquerier

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"strings"
	"sync"
)

const columnWildcard = "*"

type selectQueryContext struct {
	logger    logger.Logger
	container *v3io.Container
	workers   int

	mint, maxt  int64
	step        int64
	filter      string
	shardId     int // TODO: for query sharding/distribution
	totalShards int

	// to remove later, replace w column definitions
	functions, name string
	windows         []int

	// TODO: create columns spec from select query params
	columnsSpec         []columnMeta
	columnsSpecByMetric map[string][]columnMeta
	isAllColumns        bool
	totalColumns        int

	disableAllAggr    bool
	disableClientAggr bool

	queries         []*partQuery
	dataFrames      map[uint64]*dataFrame
	frameList       []*dataFrame
	requestChannels []chan *qryResults
	wg              sync.WaitGroup

	timeColumn Column
}

func (s *selectQueryContext) start(parts []*partmgr.DBPartition, params *SelectParams) (*frameIterator, error) {
	s.dataFrames = make(map[uint64]*dataFrame)
	queries := make([]*partQuery, len(parts))

	s.functions = params.Functions
	err := s.createColumnSpecs(params)
	if err != nil {
		return nil, err
	}

	for i, part := range parts {
		qry, err := s.queryPartition(part)
		if err != nil {
			return nil, err
		}
		queries[i] = qry
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

	for _, df := range s.frameList {
		df.CalculateColumns()
	}

	s.totalColumns = s.frameList[0].Len()
	frameIter := NewFrameIterator(s)

	return frameIter, nil
}

// Query a single partition
func (s *selectQueryContext) queryPartition(partition *partmgr.DBPartition) (*partQuery, error) {

	mint, maxt := partition.GetPartitionRange()
	step := s.step

	if s.maxt < maxt {
		maxt = s.maxt
	}

	if s.mint > mint {
		mint = s.mint
	}

	newQuery := &partQuery{mint: mint, maxt: maxt, partition: partition, step: step}
	functions := s.functions
	if functions == "" && step > 0 && step >= partition.RollupTime() && partition.AggrType().HasAverage() {
		functions = "avg"
	}

	// Check whether there are aggregations to add and aggregates aren't disabled
	if functions != "" && !s.disableAllAggr {

		// If step isn't passed (e.g., when using the console), the step is the
		// difference between the end (maxt) and start (mint) times (e.g., 5 minutes)
		if step == 0 {
			step = maxt - mint
		}

		if step > partition.RollupTime() && s.disableClientAggr {
			step = partition.RollupTime()
		}

		aggregationParams, err := aggregate.NewAggregationParams(functions,
			"v",
			partition.AggrBuckets(),
			step,
			partition.RollupTime(),
			s.windows)

		if err != nil {
			return nil, err
		}

		// TODO: init/use a new aggr objects
		newQuery.preAggregated = aggregationParams.CanAggregate(partition.AggrType())
		if newQuery.preAggregated || !s.disableClientAggr {
			newQuery.aggregationParams = aggregationParams
		}
	}

	// TODO: name may become a list and separated to multiple GetItems range queries
	err := newQuery.getItems(s, s.name)

	return newQuery, err
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
		lsetAttr, lok := query.GetField("_lset").(string)
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
		encoding, nok := query.GetField("_enc").(int)
		if !nok {
			encoding = 0
		}

		results := qryResults{name: name, encoding: int16(encoding), query: query, fields: query.GetFields()}
		sort.Sort(lset) // maybe skipped if its written sorted
		hash := lset.Hash()

		// find or create data frame
		frame, ok := s.dataFrames[hash]
		if !ok {
			frame = NewDataFrame(s.columnsSpec, s.getOrCreateTimeColumn(), lset, hash, s.isRawQuery(), s.isAllColumns)
			s.dataFrames[hash] = frame
			s.frameList = append(s.frameList, frame)
		}

		results.frame = frame

		workerNum := hash & uint64(s.workers-1)
		s.requestChannels[workerNum] <- &results
	}

	return query.Err()
}

func (s *selectQueryContext) createColumnSpecs(params *SelectParams) error {
	s.columnsSpec = []columnMeta{}
	s.columnsSpecByMetric = make(map[string][]columnMeta)

	for _, col := range params.getRequestedColumns() {
		_, ok := s.columnsSpecByMetric[col.metric]
		if !ok {
			s.columnsSpecByMetric[col.metric] = []columnMeta{}
		}

		colMeta := columnMeta{metric: col.metric, alias: col.alias, interpolator: 0} // todo - change interpolator

		if col.function != "" {
			aggr, err := aggregate.AggregateFromString(col.function)
			if err != nil {
				return err
			}
			colMeta.function = aggr
		}
		s.columnsSpecByMetric[col.metric] = append(s.columnsSpecByMetric[col.metric], colMeta)
		s.columnsSpec = append(s.columnsSpec, colMeta)
		s.isAllColumns = s.isAllColumns || col.metric == columnWildcard
	}

	// Adding hidden columns if needed
	for metric, cols := range s.columnsSpecByMetric {
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
				s.columnsSpec = append(s.columnsSpec, hiddenCol)
				s.columnsSpecByMetric[metric] = append(s.columnsSpecByMetric[metric], hiddenCol)
			}
		}
	}

	if len(s.columnsSpec) == 0 {
		return errors.Errorf("no Columns were specified for query: %v", params)
	}
	return nil
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
	size := int((s.maxt-s.mint)/s.step + 1)
	timeBuckets := make([]int64, size)
	i := 0
	for t := s.mint; t <= s.maxt; t += s.step {
		timeBuckets[i] = t
		i++
	}

	columnMeta := columnMeta{metric: "time"}
	return &dataColumn{data: timeBuckets, name: "time", size: size, spec: columnMeta}
}

func (s *selectQueryContext) isRawQuery() bool { return s.functions == "" }

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

func (s *partQuery) getItems(ctx *selectQueryContext, name string) error {

	path := s.partition.GetTablePath()
	shardingKeys := []string{}
	if name != "" {
		shardingKeys = s.partition.GetShardingKeys(name)
	}
	attrs := []string{"_lset", "_enc", "_name", "_maxtime"}

	if s.preAggregated {
		s.attrs = s.aggregationParams.GetAttrNames()
	} else {
		s.attrs, s.chunk0Time = s.partition.Range2Attrs("v", s.mint, s.maxt)
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
	return s.iter.Next()
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
