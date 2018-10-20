package pquerier

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
	"time"
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

type SelectParams struct {
	Name      string
	Functions string
	Step      int64
	Windows   []int
	Filter    string

	disableAllAggr    bool
	disableClientAggr bool
}

// Base query function
func (q *V3ioQuerier) SelectQry(params *SelectParams) (set SeriesSet, err error) {

	err = q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nullSeriesSet{}, errors.Wrap(err, "Failed to read/update the TSDB schema.")
	}

	set = nullSeriesSet{}

	q.logger.Debug("Select query:\n\tMetric: %s\n\tStart Time: %s (%d)\n\tEnd Time: %s (%d)\n\tFunction: %s\n\t"+
		"Step: %d\n\tFilter: %s\n\tWindows: %v\n\tDisable All Aggr: %t\n\tDisable Client Aggr: %t",
		params.Name, time.Unix(q.mint/1000, 0).String(), q.mint, time.Unix(q.maxt/1000, 0).String(),
		q.maxt, params.Functions, params.Step,
		params.Filter, params.Windows, params.disableAllAggr, params.disableClientAggr)

	q.performanceReporter.WithTimer("QueryTimer", func() {
		params.Filter = strings.Replace(params.Filter, "__name__", "_name", -1)

		parts := q.partitionMngr.PartsForRange(q.mint, q.maxt)
		if len(parts) == 0 {
			return
		}

		if len(parts) == 1 {
			set, err = q.queryNumericPartition(parts[0], params)
			return
		}

		sets := make([]SeriesSet, len(parts))
		for i, part := range parts {
			set, err = q.queryNumericPartition(part, params)
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
func (q *V3ioQuerier) queryNumericPartition(partition *partmgr.DBPartition, params *SelectParams) (SeriesSet, error) {

	mint, maxt := partition.GetPartitionRange()
	step := params.Step

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

	newGetter := &partGetter{mint: mint, maxt: maxt, partition: partition, logger: q.logger}

	// If there are no aggregation functions and the aggregation-interval (step)
	// size is greater than the stored aggregate, use the Average aggregate.
	// TODO: When not using the Prometheus TSDB, we may want an avg aggregate
	// for any step>0 in the Prometheus range vectors using seek, and it would
	// be inefficient to use an avg aggregate.
	functions := params.Functions
	if functions == "" && step > 0 && step >= partition.RollupTime() && partition.AggrType().HasAverage() {
		functions = "avg"
	}

	// Check whether there are aggregations to add and aggregates aren't disabled
	if functions != "" && !params.disableAllAggr {

		// If step isn't passed (e.g., when using the console), the step is the
		// difference between the end (maxt) and start (mint) times (e.g., 5 minutes)
		if step == 0 {
			step = maxt - mint
		}

		if step > partition.RollupTime() && params.disableClientAggr {
			step = partition.RollupTime()
		}

		newAggrSeries, err := aggregate.NewAggregateSeries(functions,
			"v",
			partition.AggrBuckets(),
			step,
			partition.RollupTime(),
			params.Windows)

		if err != nil {
			return nil, err
		}

		// Use aggregates if possible on the TSDB side or if client aggregation
		// is enabled (Prometheus is disabled on the client side)
		newSet.canAggregate = newAggrSeries.CanAggregate(partition.AggrType())
		if newSet.canAggregate || !params.disableClientAggr {
			newSet.aggrSeries = newAggrSeries
			newSet.interval = step
			newSet.aggrIdx = newAggrSeries.NumFunctions() - 1
			newSet.overlapWin = params.Windows
			newSet.noAggrLbl = params.disableClientAggr // Don't add an "Aggregate" label in Prometheus (see aggregate.AggregateLabel)
		}
	}

	err := newSet.getItems(partition, params.Name, params.Filter, q.container, q.cfg.QryWorkers)

	return newSet, err
}

type partGetter struct {
	logger    logger.Logger
	partition *partmgr.DBPartition
	iter      utils.ItemsCursor

	mint, maxt    int64
	bucket0Time   int64
	bucketTime    int64
	preAggregated bool
	attrs         []string

	aggrSeries   *aggregate.AggregateSeries
	canAggregate bool
}

func (s *partGetter) getItems(partition *partmgr.DBPartition, name, filter string, container *v3io.Container, workers int) error {

	path := partition.GetTablePath()
	shardingKeys := []string{}
	if name != "" {
		shardingKeys = partition.GetShardingKeys(name)
	}
	attrs := []string{"_lset", "_ooo", "_name", "_maxtime"}

	if s.aggrSeries != nil && s.canAggregate {
		s.attrs = s.aggrSeries.GetAttrNames()
	} else {
		s.attrs, s.bucket0Time = s.partition.Range2Attrs("v", s.mint, s.maxt)
	}
	attrs = append(attrs, s.attrs...)

	s.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", filter, "name", name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: filter, ShardingKey: name}
	iter, err := utils.NewAsyncItemsCursor(container, &input, workers, shardingKeys, s.logger)
	if err != nil {
		return err
	}

	s.iter = iter
	return nil
}
