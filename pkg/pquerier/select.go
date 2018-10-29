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
			newQuery.aggrParams = aggregationParams
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
			frame = &dataFrame{lset: lset, hash: hash, isRawSeries: s.functions == ""}
			// TODO: init dataframe columns ..
			var numberOfColumns int64
			if !frame.isRawSeries {
				numberOfColumns = (s.maxt-s.mint)/s.step + 1
			} else {
				numberOfColumns = 100
			}
			frame.columns = make([]Column, 0, numberOfColumns)

			frame.byName = make(map[string]int, numberOfColumns)
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

	// Directly getting the column specs
	if params.columnSpecs != nil && len(params.columnSpecs) > 0 {
		s.columnsSpec = params.columnSpecs
		// Creating map of column specs by metric name
		for _, col := range params.columnSpecs {
			_, ok := s.columnsSpecByMetric[col.metric]
			if !ok {
				s.columnsSpecByMetric[col.metric] = []columnMeta{}
			}
			s.columnsSpecByMetric[col.metric] = append(s.columnsSpecByMetric[col.metric], col)
			s.isAllColumns = s.isAllColumns || col.metric == columnWildcard
		}

		// Adding hidden columns if needed
		for metric, cols := range s.columnsSpecByMetric {
			var metricMask aggregate.AggrType
			var aggregates []aggregate.AggrType
			for _, colSpec := range cols {
				metricMask |= colSpec.function
				aggregates = append(aggregates, colSpec.function)
			}

			hiddenColumns := aggregate.GetHiddenAggregates(metricMask, aggregates)
			for _, hiddenAggr := range hiddenColumns {
				hiddenCol := columnMeta{metric: metric, function: hiddenAggr, isHidden: true}
				s.columnsSpec = append(s.columnsSpec, hiddenCol)
				s.columnsSpecByMetric[metric] = append(cols, hiddenCol)
			}
		}
		// Create column specs from metric name and aggregation
	} else {
		if params.Functions == "" {
			s.columnsSpec = append(s.columnsSpec, columnMeta{metric: params.Name})
		} else {
			mask, aggregates, err := aggregate.StrToAggr(params.Functions)
			if err != nil {
				return err
			}
			for _, aggr := range aggregates {
				s.columnsSpec = append(s.columnsSpec, columnMeta{metric: params.Name, function: aggr})
			}

			hiddenColumns := aggregate.GetHiddenAggregates(mask, aggregates)
			for _, hiddenAggr := range hiddenColumns {
				s.columnsSpec = append(s.columnsSpec, columnMeta{metric: params.Name, function: hiddenAggr, isHidden: true})
			}
		}

		s.columnsSpecByMetric[params.Name] = s.columnsSpec
		s.isAllColumns = params.Name == columnWildcard
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

func (s *selectQueryContext) createDataFrame(lset utils.Labels, hash uint64) *dataFrame {
	var df *dataFrame
	// is raw query
	if s.functions == "" {
		df = &dataFrame{lset: lset, hash: hash, isRawSeries: s.functions == ""}
		df.byName = make(map[string]int, 100)
	} else {
		df.byName = make(map[string]int, len(s.columnsSpec))
		df.columns = make([]Column, 0, len(s.columnsSpec))
		if !s.isAllColumns {
			for i, col := range s.columnsSpec {
				df.columns = append(df.columns, &dataColumn{name: col.getColumnName()})
				df.byName[col.getColumnName()] = i
			}
		}
	}

	return df
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

	chunk0Time    int64
	chunkTime     int64
	preAggregated bool
	aggrParams    *aggregate.AggregationParams
}

func (s *partQuery) getItems(ctx *selectQueryContext, name string) error {

	path := s.partition.GetTablePath()
	shardingKeys := []string{}
	if name != "" {
		shardingKeys = s.partition.GetShardingKeys(name)
	}
	attrs := []string{"_lset", "_enc", "_name", "_maxtime"}

	if s.preAggregated {
		s.attrs = s.aggrParams.GetAttrNames()
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
