package pquerier

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"strings"
	"sync"
	"time"
)

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
	columnsSpec  *map[string][]columnMeta
	totalColumns int

	disableAllAggr    bool
	disableClientAggr bool

	queries         []*partQuery
	dataFrames      map[uint64]*dataFrame
	frameList       []*dataFrame
	requestChannels []chan *qryResults
	wg              sync.WaitGroup
}

func (s *selectQueryContext) start(parts []*partmgr.DBPartition, params *SelectParams) (*frameIterator, error) {

	queries := make([]*partQuery, len(parts))
	for i, part := range parts {
		var qry *partQuery
		qry, err := s.queryPartition(part)
		if err != nil {
			return nil, err
		}
		queries[i] = qry
	}

	err := s.startCollectors()
	if err != nil {
		return nil, err
	}

	for _, query := range queries {
		err = s.processQueryResults(query)
		if err != nil {
			return nil, err
		}
	}

	// wait for Go routines to complete
	s.wg.Wait()

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

	newQuery := &partQuery{mint: mint, maxt: maxt, partition: partition}
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

		newAggrSeries, err := aggregate.NewAggregateSeries(functions,
			"v",
			partition.AggrBuckets(),
			step,
			partition.RollupTime(),
			s.windows)

		if err != nil {
			return nil, err
		}

		// TODO: init/use a new aggr objects
		newQuery.preAggregated = newAggrSeries.CanAggregate(partition.AggrType())
		if newQuery.preAggregated || !s.disableClientAggr {
			newQuery.aggrSeries = newAggrSeries
		}
	}

	// TODO: name may become a list and separated to multiple GetItems range queries
	err := newQuery.getItems(s, s.name)

	return newQuery, err
}

// TODO: replace with a real collector implementation
func dummyCollector(ctx *selectQueryContext, index int) {
	defer ctx.wg.Done()

	fmt.Println("starting collector:", index)
	time.Sleep(5 * time.Second)

	// collector should have a loop waiting on the s.requestChannels[index] and processing requests
	// once the chan is closed or a fin request arrived we exit
}

func (s *selectQueryContext) startCollectors() error {

	s.requestChannels = make([]chan *qryResults, s.workers)

	for i := 0; i < s.workers; i++ {
		newChan := make(chan *qryResults, 1000)
		s.requestChannels[i] = newChan

		// Increment the WaitGroup counter.
		s.wg.Add(1)

		go func() {
			dummyCollector(s, i)
		}()
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
			frame = &dataFrame{lset: lset, hash: hash}
			// TODO: init dataframe columns ..

			s.dataFrames[hash] = frame
			s.frameList = append(s.frameList, frame)
		}

		results.frame = frame

		workerNum := hash & uint64(s.workers-1)
		s.requestChannels[workerNum] <- &results
	}

	return query.Err()
}

// query object for a single partition (or name and partition in future optimizations)

type partQuery struct {
	partition *partmgr.DBPartition
	iter      utils.ItemsCursor
	partIndex int

	baseTime   int64
	mint, maxt int64
	attrs      []string

	chunk0Time    int64
	chunkTime     int64
	preAggregated bool
	aggrSeries    *aggregate.AggregateSeries
}

func (s *partQuery) getItems(ctx *selectQueryContext, name string) error {

	path := s.partition.GetTablePath()
	shardingKeys := []string{}
	if name != "" {
		shardingKeys = s.partition.GetShardingKeys(name)
	}
	attrs := []string{"_lset", "_enc", "_name", "_maxtime"}

	if s.preAggregated {
		s.attrs = s.aggrSeries.GetAttrNames()
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
	return false
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
