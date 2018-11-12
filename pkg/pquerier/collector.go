package pquerier

import (
	"encoding/binary"
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"math"
	"time"
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
	  else
		?

	init child raw-chunk or attribute iterators
	iterate over data and fill bucketed arrays
		if missing time or data use interpolation

- if got fin message and processed last item
	use sync waitgroup to signal the main that the go routines are done
	will allow main flow to continue and serve the results, no locks are required

*/

// TODO: replace with a real collector implementation
func dummyCollector(ctx *selectQueryContext, index int) {
	defer ctx.wg.Done()

	fmt.Println("starting collector:", index)
	time.Sleep(5 * time.Second)

	// collector should have a loop waiting on the s.requestChannels[index] and processing requests
	// once the chan is closed or a fin request arrived we exit
}

// Main collector which processes query results from a channel and then dispatches them according to query type.
// Query types: raw data, server-side aggregates, client-side aggregates
func mainCollector(ctx *selectQueryContext, index int) {
	defer ctx.wg.Done()

	for res := range ctx.requestChannels[index] {
		if res.IsRawQuery() {
			rawCollector(res)
		} else if res.IsServerAggregates() {
			aggregateServerAggregates(ctx, res)
		} else if res.IsClientAggregates() {
			aggregateClientAggregates(ctx, res)
		} else if res.IsDownsample() {
			downsampleRawData(ctx, res)
		}
	}
}

func rawCollector(res *qryResults) {
	frameIndex, ok := res.frame.columnByName[res.name]
	if ok {
		res.frame.rawColumns[frameIndex].(*V3ioRawSeries).AddChunks(res)
	} else {
		res.frame.rawColumns = append(res.frame.rawColumns, NewRawSeries(res))
		res.frame.columnByName[res.name] = len(res.frame.rawColumns) - 1
	}
}

func aggregateClientAggregates(ctx *selectQueryContext, res *qryResults) {
	it := newRawChunkIterator(*res, nil)
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
	for _, col := range res.frame.columns {
		if col.GetColumnSpec().metric == res.name && col.GetColumnSpec().isConcrete() {
			array, ok := res.fields[aggregate.ToAttrName(col.GetColumnSpec().function)]
			if !ok {
				ctx.logger.Error("requested function %v was not found in response", col.GetColumnSpec().function)
			} else {
				// go over the byte array and convert each uint as we go to save memory allocation
				bytes := array.([]byte)
				for i := 16; i+8 <= len(bytes); i += 8 {
					val := binary.LittleEndian.Uint64(bytes[i : i+8])
					currentValueIndex := (i - 16) / 8
					currentValueTime := res.query.partition.GetStartTime() + int64(currentValueIndex+1)*res.query.aggregationParams.RollupTime
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

func downsampleRawData(ctx *selectQueryContext, res *qryResults) {
	it := newRawChunkIterator(*res, nil).(*rawChunkIterator)
	col := res.frame.columns[res.frame.columnByName[res.name]]
	for currBucket := 0; currBucket < col.Len(); currBucket++ {
		currBucketTime := int64(currBucket)*ctx.step + ctx.mint
		if it.Seek(currBucketTime) {
			t, v := it.At()
			if t == currBucketTime {
				col.SetDataAt(currBucket, v)
			} else {
				prevT, prevV := it.PeakBack()

				tollerance := 2 * res.query.step
				if prevT != 0 && t-prevT > tollerance {
					col.SetDataAt(currBucket, math.NaN())
				} else {
					_, interpolatedV := col.GetInterpolationFunction()(prevT, t, currBucketTime, prevV, v)
					col.SetDataAt(currBucket, interpolatedV)
				}
			}
		} else {
			col.SetDataAt(currBucket, math.NaN())
		}
	}
}
