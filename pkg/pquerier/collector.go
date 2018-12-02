package pquerier

import (
	"encoding/binary"
	"math"

	"github.com/v3io/v3io-tsdb/pkg/aggregate"
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

	lastTimePerMetric := make(map[string]int64, len(ctx.columnsSpecByMetric))
	lastValuePerMetric := make(map[string]float64, len(ctx.columnsSpecByMetric))

	for res := range responseChannel {
		if res.IsRawQuery() {
			rawCollector(ctx, res)
		} else {
			err := res.frame.addMetricIfNotExist(res.name, ctx.getResultBucketsSize(), res.IsServerAggregates())
			if err != nil {
				ctx.logger.Error("problem adding new metric '%v', lset: %v, err:%v", res.name, res.frame.lset, err)
				ctx.errorChannel <- err
				return
			}

			if res.IsServerAggregates() {
				aggregateServerAggregates(ctx, res)
			} else if res.IsClientAggregates() {
				aggregateClientAggregates(ctx, res)
			}

			// It is possible to query an aggregate and down sample raw chunks in the same df.
			if res.IsDownsample() {
				lastTimePerMetric[res.name], lastValuePerMetric[res.name], err = downsampleRawData(ctx, res, lastTimePerMetric[res.name], lastValuePerMetric[res.name])
				if err != nil {
					ctx.logger.Error("problem downsampling '%v', lset: %v, err:%v", res.name, res.frame.lset, err)
					ctx.errorChannel <- err
					return
				}
			}
		}
	}
}

func rawCollector(ctx *selectQueryContext, res *qryResults) {
	frameIndex, ok := res.frame.columnByName[res.name]
	if ok {
		res.frame.rawColumns[frameIndex].(*V3ioRawSeries).AddChunks(res)
	} else {
		series, err := NewRawSeries(res, ctx.logger.GetChild("v3ioRawSeries"))
		if err != nil {
			ctx.errorChannel <- err
			return
		}
		res.frame.rawColumns = append(res.frame.rawColumns, series)
		res.frame.columnByName[res.name] = len(res.frame.rawColumns) - 1
	}
}

func aggregateClientAggregates(ctx *selectQueryContext, res *qryResults) {
	it := newRawChunkIterator(res, nil)
	for it.Next() {
		t, v := it.At()
		currentCell := (t - ctx.mint) / res.query.aggregationParams.Interval

		for _, col := range res.frame.columns {
			if col.GetColumnSpec().metric == res.name {
				col.SetDataAt(int(currentCell), v)
			}
		}
	}
}

func aggregateServerAggregates(ctx *selectQueryContext, res *qryResults) {
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
					currentValueTime := partitionStartTime + int64(currentValueIndex+1)*rollupInterval
					currentCell := (currentValueTime - ctx.mint) / res.query.aggregationParams.Interval

					var floatVal float64
					if aggregate.IsCountAggregate(col.GetColumnSpec().function) {
						floatVal = float64(val)
					} else {
						floatVal = math.Float64frombits(val)
					}
					col.SetDataAt(int(currentCell), floatVal)
				}
			}
		}
	}
}

func downsampleRawData(ctx *selectQueryContext, res *qryResults,
	previousPartitionLastTime int64, previousPartitionLastValue float64) (int64, float64, error) {
	var lastT int64
	var lastV float64
	it := newRawChunkIterator(res, nil).(*rawChunkIterator)
	col, err := res.frame.Column(res.name)
	if err != nil {
		return previousPartitionLastTime, previousPartitionLastValue, err
	}
	for currBucket := 0; currBucket < col.Len(); currBucket++ {
		currBucketTime := int64(currBucket)*ctx.step + ctx.mint
		if it.Seek(currBucketTime) {
			t, v := it.At()
			tBucketIndex := (t - ctx.mint) / ctx.step
			if t == currBucketTime {
				col.SetDataAt(currBucket, v)
			} else if tBucketIndex == int64(currBucket) {
				prevT, prevV := it.PeakBack()

				// In case it's the first point in the partition use the last point of the previous partition for the interpolation
				if prevT == 0 {
					prevT = previousPartitionLastTime
					prevV = previousPartitionLastValue
				}
				// If previous point is too old for interpolation
				interpolateFunc, tolerance := col.GetInterpolationFunction()
				if prevT != 0 && t-prevT > tolerance {
					col.SetDataAt(currBucket, math.NaN())
				} else {
					_, interpolatedV := interpolateFunc(prevT, t, currBucketTime, prevV, v)
					col.SetDataAt(currBucket, interpolatedV)
				}
			} else {
				col.SetDataAt(currBucket, math.NaN())
			}
		} else {
			lastT, lastV = it.At()
			col.SetDataAt(currBucket, math.NaN())
		}
	}

	return lastT, lastV, nil
}
