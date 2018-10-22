package pquerier

import (
	"fmt"
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
