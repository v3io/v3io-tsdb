package pquerier

import (
	"encoding/binary"
	"math"

	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

/* main query flow logic

fire GetItems to all partitions and tables

iterate over results from first to last partition
	hash lookup (over labels only w/o name) to find dataFrame
		if not found create new dataFrame
	based on hash dispatch work to one of the parallel collectors
	collectors convert raw/array data to series and aggregate or group

once collectors are done (wg.done) return SeriesSet (prom compatible) or FrameSet (iguazio column interface)
	final aggregators (Avg, Stddav/var, ..) are formed from raw aggr in flight via iterators
	- Series: have a single name and optional aggregator per time series, values limited to Float64
	- Frames: have index/time column(s) and multiple named/typed value columns (one per metric name * function)

** optionally can return SeriesSet (from dataFrames) to Prom immediately after we completed GetItems iterators
   and block (wg.done) the first time Prom tries to access the SeriesIter data (can lower latency)

   if result set needs to be ordered we can also sort the dataFrames based on Labels data (e.g. order-by username)
   in parallel to having all the time series data processed by the collectors

*/

/*  collector logic:

- get qryResults from chan

- if raw query
	if first partition
		create series
	  else
		append chunks to existing series

- if vector query (results are bucketed over time or grouped by)
	if first partition
		create & init array per function (and per name) based on query metadata/results

	init child raw-chunk or attribute iterators
	iterate over data and fill bucketed arrays
		if missing time or data use interpolation

- if got fin message and processed last item
	use sync waitgroup to signal the main that the go routines are done
	will allow main flow to continue and serve the results, no locks are required

*/

// Main collector which processes query results from a channel and then dispatches them according to query type.
// Query types: raw data, server-side aggregates, client-side aggregates
func mainCollector(ctx *selectQueryContext, responseChannel chan *qryResults) {
	defer ctx.wg.Done()

	lastTimePerMetric := make(map[uint64]int64, len(ctx.columnsSpecByMetric))
	lastValuePerMetric := make(map[uint64]float64, len(ctx.columnsSpecByMetric))

	for {
		select {
		case _ = <-ctx.stopChan:
			return
		case res, ok := <-responseChannel:
			if !ok {
				return
			}
			if res.IsRawQuery() {
				err := rawCollector(ctx, res)
				if err != nil {
					ctx.errorChannel <- err
					return
				}
			} else {
				err := res.frame.addMetricIfNotExist(res.name, ctx.getResultBucketsSize(), res.IsServerAggregates())
				if err != nil {
					ctx.logger.Error("problem adding new metric '%v', lset: %v, err:%v", res.name, res.frame.lset, err)
					ctx.errorChannel <- err
					return
				}
				lsetAttr, _ := res.fields[config.LabelSetAttrName].(string)
				lset, _ := utils.LabelsFromString(lsetAttr)
				lset = append(lset, utils.Label{Name: config.MetricNameAttrName, Value: res.name})
				currentResultHash := lset.Hash()

				// Aggregating cross series aggregates, only supported over raw data.
				if ctx.isCrossSeriesAggregate {
					lastTimePerMetric[currentResultHash], lastValuePerMetric[currentResultHash], _ = aggregateClientAggregatesCrossSeries(ctx, res, lastTimePerMetric[currentResultHash], lastValuePerMetric[currentResultHash])
				} else {
					// Aggregating over time aggregates
					if res.IsServerAggregates() {
						aggregateServerAggregates(ctx, res)
					} else if res.IsClientAggregates() {
						aggregateClientAggregates(ctx, res)
					}
				}

				// It is possible to query an aggregate and down sample raw chunks in the same df.
				if res.IsDownsample() {
					lastTimePerMetric[currentResultHash], lastValuePerMetric[currentResultHash], err = downsampleRawData(ctx, res, lastTimePerMetric[currentResultHash], lastValuePerMetric[currentResultHash])
					if err != nil {
						ctx.logger.Error("problem downsampling '%v', lset: %v, err:%v", res.name, res.frame.lset, err)
						ctx.errorChannel <- err
						return
					}
				}
			}
		}
	}
}

func rawCollector(ctx *selectQueryContext, res *qryResults) error {
	ctx.logger.Debug("using Raw Collector for metric %v", res.name)

	if res.frame.isWildcardSelect {
		columnIndex, ok := res.frame.columnByName[res.name]
		if ok {
			res.frame.rawColumns[columnIndex].(*V3ioRawSeries).AddChunks(res)
		} else {
			series, err := NewRawSeries(res, ctx.logger.GetChild("v3ioRawSeries"))
			if err != nil {
				return err
			}
			res.frame.rawColumns = append(res.frame.rawColumns, series)
			res.frame.columnByName[res.name] = len(res.frame.rawColumns) - 1
		}
	} else {
		columnIndex := res.frame.columnByName[res.name]
		rawColumn := res.frame.rawColumns[columnIndex]
		if rawColumn != nil {
			res.frame.rawColumns[columnIndex].(*V3ioRawSeries).AddChunks(res)
		} else {
			series, err := NewRawSeries(res, ctx.logger.GetChild("v3ioRawSeries"))
			if err != nil {
				return err
			}
			res.frame.rawColumns[columnIndex] = series
		}
	}
	return nil
}

func aggregateClientAggregates(ctx *selectQueryContext, res *qryResults) {
	ctx.logger.Debug("using Client Aggregates Collector for metric %v", res.name)
	it := newRawChunkIterator(res, ctx.logger)
	for it.Next() {
		t, v := it.At()

		if res.query.aggregationParams.HasAggregationWindow() {
			windowAggregation(ctx, res, t, v)
		} else {
			intervalAggregation(ctx, res, t, v)
		}
	}
}

func aggregateServerAggregates(ctx *selectQueryContext, res *qryResults) {
	ctx.logger.Debug("using Server Aggregates Collector for metric %v", res.name)

	partitionStartTime := res.query.partition.GetStartTime()
	rollupInterval := res.query.aggregationParams.GetRollupTime()
	for _, col := range res.frame.columns {
		if col.GetColumnSpec().metric == res.name &&
			aggregate.HasAggregates(col.GetColumnSpec().function) &&
			col.GetColumnSpec().isConcrete() {

			array, ok := res.fields[aggregate.ToAttrName(col.GetColumnSpec().function)]
			if !ok {
				ctx.logger.Warn("requested function %v was not found in response", col.GetColumnSpec().function)
			} else {
				// go over the byte array and convert each uint as we go to save memory allocation
				bytes := array.([]byte)

				for i := 16; i+8 <= len(bytes); i += 8 {
					val := binary.LittleEndian.Uint64(bytes[i : i+8])
					currentValueIndex := (i - 16) / 8

					// Calculate server side aggregate bucket by its median time
					currentValueTime := partitionStartTime + int64(currentValueIndex)*rollupInterval + rollupInterval/2
					currentCell := (currentValueTime - ctx.queryParams.From) / res.query.aggregationParams.Interval

					var floatVal float64
					if aggregate.IsCountAggregate(col.GetColumnSpec().function) {
						floatVal = float64(val)
					} else {
						floatVal = math.Float64frombits(val)
					}

					bottomMargin := res.query.aggregationParams.Interval
					if res.query.aggregationParams.HasAggregationWindow() {
						bottomMargin = res.query.aggregationParams.GetAggregationWindow()
					}
					if currentValueTime >= ctx.queryParams.From-bottomMargin && currentValueTime <= ctx.queryParams.To+res.query.aggregationParams.Interval {
						if !res.query.aggregationParams.HasAggregationWindow() {
							_ = res.frame.setDataAt(col.Name(), int(currentCell), floatVal)
						} else {
							windowAggregationWithServerAggregates(ctx, res, col, currentValueTime, floatVal)
						}
					}
				}
			}
		}
	}
}

func downsampleRawData(ctx *selectQueryContext, res *qryResults,
	previousPartitionLastTime int64, previousPartitionLastValue float64) (int64, float64, error) {
	ctx.logger.Debug("using Downsample Collector for metric %v", res.name)

	it, ok := newRawChunkIterator(res, ctx.logger).(*RawChunkIterator)
	if !ok {
		return previousPartitionLastTime, previousPartitionLastValue, nil
	}
	col, err := res.frame.Column(res.name)
	if err != nil {
		return previousPartitionLastTime, previousPartitionLastValue, err
	}
	for currCell := 0; currCell < col.Len(); currCell++ {
		currCellTime := int64(currCell)*ctx.queryParams.Step + ctx.queryParams.From
		prev, err := col.getBuilder().At(currCell)

		// Only update a cell if it hasn't been set yet
		if prev == nil || err != nil {
			if it.Seek(currCellTime) {
				t, v := it.At()
				if t == currCellTime {
					_ = res.frame.setDataAt(col.Name(), currCell, v)
				} else {
					prevT, prevV := it.PeakBack()

					// In case it's the first point in the partition use the last point of the previous partition for the interpolation
					if prevT == 0 {
						prevT = previousPartitionLastTime
						prevV = previousPartitionLastValue
					}
					interpolatedT, interpolatedV := col.GetInterpolationFunction()(prevT, t, currCellTime, prevV, v)

					// Check if the interpolation was successful in terms of exceeding tolerance
					if !(interpolatedT == 0 && interpolatedV == 0) {
						_ = res.frame.setDataAt(col.Name(), currCell, interpolatedV)
					}
				}
			}
		}
	}

	lastT, lastV := it.At()
	return lastT, lastV, nil
}

func aggregateClientAggregatesCrossSeries(ctx *selectQueryContext, res *qryResults, previousPartitionLastTime int64, previousPartitionLastValue float64) (int64, float64, error) {
	ctx.logger.Debug("using Client Aggregates Collector for metric %v", res.name)
	it, ok := newRawChunkIterator(res, ctx.logger).(*RawChunkIterator)
	if !ok {
		return previousPartitionLastTime, previousPartitionLastValue, nil
	}

	var previousPartitionEndBucket int
	if previousPartitionLastTime != 0 {
		previousPartitionEndBucket = int((previousPartitionLastTime-ctx.queryParams.From)/ctx.queryParams.Step) + 1
	}
	maxBucketForPartition := int((res.query.partition.GetEndTime() - ctx.queryParams.From) / ctx.queryParams.Step)
	if maxBucketForPartition > ctx.getResultBucketsSize() {
		maxBucketForPartition = ctx.getResultBucketsSize()
	}

	for currBucket := previousPartitionEndBucket; currBucket < maxBucketForPartition; currBucket++ {
		currBucketTime := int64(currBucket)*ctx.queryParams.Step + ctx.queryParams.From

		if it.Seek(currBucketTime) {
			t, v := it.At()
			if t == currBucketTime {
				for _, col := range res.frame.columns {
					if col.GetColumnSpec().metric == res.name {
						_ = res.frame.setDataAt(col.Name(), currBucket, v)
					}
				}
			} else {
				prevT, prevV := it.PeakBack()

				// In case it's the first point in the partition use the last point of the previous partition for the interpolation
				if prevT == 0 {
					prevT = previousPartitionLastTime
					prevV = previousPartitionLastValue
				}

				for _, col := range res.frame.columns {
					if col.GetColumnSpec().metric == res.name {
						interpolatedT, interpolatedV := col.GetInterpolationFunction()(prevT, t, currBucketTime, prevV, v)
						if !(interpolatedT == 0 && interpolatedV == 0) {
							_ = res.frame.setDataAt(col.Name(), currBucket, interpolatedV)
						}
					}
				}
			}
		} else {
			break
		}
	}

	lastT, lastV := it.At()
	return lastT, lastV, nil
}

func intervalAggregation(ctx *selectQueryContext, res *qryResults, t int64, v float64) {
	currentCell := getRelativeCell(t, ctx.queryParams.From, res.query.aggregationParams.Interval, false)
	aggregateAllColumns(res, currentCell, v)
}

func windowAggregation(ctx *selectQueryContext, res *qryResults, t int64, v float64) {
	currentCell := getRelativeCell(t, ctx.queryParams.From, res.query.aggregationParams.Interval, true)
	aggregationWindow := res.query.aggregationParams.GetAggregationWindow()

	if aggregationWindow > res.query.aggregationParams.Interval {
		currentCellTime := ctx.queryParams.From + currentCell*res.query.aggregationParams.Interval
		maximumAffectedTime := t + aggregationWindow
		numAffectedCells := (maximumAffectedTime-currentCellTime)/res.query.aggregationParams.Interval + 1 // +1 to include the current cell

		for i := int64(0); i < numAffectedCells; i++ {
			aggregateAllColumns(res, currentCell+i, v)
		}
	} else if aggregationWindow < res.query.aggregationParams.Interval {
		if t+aggregationWindow >= ctx.queryParams.From+currentCell*res.query.aggregationParams.Interval {
			aggregateAllColumns(res, currentCell, v)
		}
	} else {
		aggregateAllColumns(res, currentCell, v)
	}
}

func windowAggregationWithServerAggregates(ctx *selectQueryContext, res *qryResults, column Column, t int64, v float64) {
	currentCell := getRelativeCell(t, ctx.queryParams.From, res.query.aggregationParams.Interval, true)

	aggregationWindow := res.query.aggregationParams.GetAggregationWindow()
	if aggregationWindow > res.query.aggregationParams.Interval {
		currentCellTime := ctx.queryParams.From + currentCell*res.query.aggregationParams.Interval
		maxAffectedTime := t + aggregationWindow
		numAffectedCells := (maxAffectedTime-currentCellTime)/res.query.aggregationParams.Interval + 1 // +1 to include the current cell

		for i := int64(0); i < numAffectedCells; i++ {
			_ = res.frame.setDataAt(column.Name(), int(currentCell+i), v)
		}
	} else {
		_ = res.frame.setDataAt(column.Name(), int(currentCell), v)
	}
}

func getRelativeCell(time, beginning, interval int64, roundUp bool) int64 {
	cell := (time - beginning) / interval

	if roundUp && (time-beginning)%interval > 0 {
		cell++
	}

	return cell
}

// Set data to all aggregated columns for the given metric
func aggregateAllColumns(res *qryResults, cell int64, value float64) {
	for _, col := range res.frame.columns {
		colSpec := col.GetColumnSpec()
		if colSpec.metric == res.name && colSpec.function != 0 {
			_ = res.frame.setDataAt(col.Name(), int(cell), value)
		}
	}
}
