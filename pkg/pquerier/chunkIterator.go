package pquerier

import (
	"github.com/nuclio/logger"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
)

// Chunk-list series iterator
type rawChunkIterator struct {
	mint, maxt int64

	chunks []chunkenc.Chunk

	chunkIndex int
	chunksMax  []int64
	iter       chunkenc.Iterator
	log        logger.Logger

	prevT int64
	prevV float64
}

func newRawChunkIterator(item qryResults, log logger.Logger) SeriesIterator {
	maxt := item.query.maxt
	maxTime := item.fields["_maxtime"]
	if maxTime != nil && int64(maxTime.(int)) < maxt {
		maxt = int64(maxTime.(int))
	}

	newIterator := rawChunkIterator{
		mint: item.query.mint, maxt: maxt, log: log}

	newIterator.AddChunks(item)

	if len(newIterator.chunks) == 0 {
		// If there's no data, create a null iterator
		return &nullSeriesIterator{}
	} else {
		newIterator.iter = newIterator.chunks[0].Iterator()
		return &newIterator
	}
}

// Advance the iterator to the specified chunk and time
func (it *rawChunkIterator) Seek(t int64) bool {

	// Seek time is after the item's end time (maxt)
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t
	if t < it.mint {
		t = it.mint
	}

	// Check the first element
	t0, _ := it.iter.At()
	if t0 > it.maxt {
		return false
	}
	if t <= t0 {
		return true
	}

	for {
		it.prevT, it.prevV = it.At()
		if it.iter.Next() {
			t0, _ := it.iter.At()
			if t0 > it.maxt {
				return false
			}
			if t > it.chunksMax[it.chunkIndex] {
				// This chunk is too far behind; move to the next chunk or
				// Return false if it's the last chunk
				if it.chunkIndex == len(it.chunks)-1 {
					return false
				}
				it.chunkIndex++
				it.iter = it.chunks[it.chunkIndex].Iterator()
			} else if t <= t0 {
				// The cursor (t0) is either on t or just passed t
				return true
			}
		} else {
			// End of chunk; move to the next chunk or return if last
			if it.chunkIndex == len(it.chunks)-1 {
				return false
			}
			it.chunkIndex++
			it.iter = it.chunks[it.chunkIndex].Iterator()
		}
	}
}

// Move to the next iterator item
func (it *rawChunkIterator) Next() bool {
	it.prevT, it.prevV = it.At()
	if it.iter.Next() {
		t, _ := it.iter.At()
		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t <= it.maxt {
			return true
		}
		return false
	}

	if err := it.iter.Err(); err != nil {
		return false
	}
	if it.chunkIndex == len(it.chunks)-1 {
		return false
	}

	it.chunkIndex++
	it.iter = it.chunks[it.chunkIndex].Iterator()
	return it.Next()
}

// Read the time and value at the current location
func (it *rawChunkIterator) At() (t int64, v float64) { return it.iter.At() }

func (it *rawChunkIterator) Err() error { return it.iter.Err() }

func (it *rawChunkIterator) AddChunks(item qryResults) {
	var chunks []chunkenc.Chunk
	var chunksMax []int64
	if item.query.maxt > it.maxt {
		it.maxt = item.query.maxt
	}
	if item.query.mint < it.mint {
		it.mint = item.query.mint
	}
	_, firstChunkTime := item.query.partition.Range2Attrs("v", it.mint, it.maxt)
	// Create and initialize a chunk encoder per chunk blob
	i := 0
	for _, attr := range item.query.attrs {
		values := item.fields[attr]

		if values != nil {
			bytes := values.([]byte)
			chunk, err := chunkenc.FromData(it.log, chunkenc.EncXOR, bytes, 0)
			if err != nil {
				it.log.ErrorWith("Error reading chunk buffer", "columns", item.query.attrs, "err", err)
			} else {
				chunks = append(chunks, chunk)
				chunksMax = append(chunksMax,
					firstChunkTime+int64(i+1)*item.query.partition.TimePerChunk()-1)
			}
		}
		i++
	}

	it.chunks = append(it.chunks, chunks...)
	it.chunksMax = append(it.chunksMax, chunksMax...)
}

func (it *rawChunkIterator) PeakBack() (t int64, v float64) { return it.prevT, it.prevV }

// Null-series iterator
type nullSeriesIterator struct {
	err error
}

func (s nullSeriesIterator) Seek(t int64) bool        { return false }
func (s nullSeriesIterator) Next() bool               { return false }
func (s nullSeriesIterator) At() (t int64, v float64) { return 0, 0 }
func (s nullSeriesIterator) Err() error               { return s.err }

func NewRawSeries(results *qryResults) Series {
	newSeries := V3ioRawSeries{fields: results.fields}
	newSeries.initLabels()
	newSeries.iter = newBucketedRawChunkIterator(*results, nil)
	return &newSeries
}

type V3ioRawSeries struct {
	fields map[string]interface{}
	lset   utils.Labels
	iter   SeriesIterator
	hash   uint64
}

func (s *V3ioRawSeries) Labels() utils.Labels { return s.lset }

// Get the unique series key for sorting
func (s *V3ioRawSeries) GetKey() uint64 {
	if s.hash == 0 {
		s.hash = s.lset.Hash()
	}
	return s.hash
}

func (s *V3ioRawSeries) Iterator() SeriesIterator { return s.iter }

func (s *V3ioRawSeries) AddChunks(results *qryResults) {
	s.iter.(*bucketedRawChunkIterator).AddChunks(results)
}

// Initialize the label set from _lset and _name attributes
func (s *V3ioRawSeries) initLabels() {
	name, nok := s.fields["_name"].(string)
	if !nok {
		name = "UNKNOWN"
	}
	lsetAttr, lok := s.fields["_lset"].(string)
	if !lok {
		lsetAttr = "UNKNOWN"
	}
	if !lok || !nok {
		//.Error("Error in initLabels; bad field values.")
	}

	lset := utils.Labels{utils.Label{Name: "__name__", Value: name}}

	splitLset := strings.Split(lsetAttr, ",")
	for _, label := range splitLset {
		kv := strings.Split(label, "=")
		if len(kv) > 1 {
			lset = append(lset, utils.Label{Name: kv[0], Value: kv[1]})
		}
	}

	s.lset = lset
}

// Chunk-list series iterator
type bucketedRawChunkIterator struct {
	iter          SeriesIterator
	log           logger.Logger
	mint, step    int64
	currentBucket int
}

func newBucketedRawChunkIterator(item qryResults, log logger.Logger) SeriesIterator {
	newIterator := bucketedRawChunkIterator{log: log, step: item.query.step, mint: item.query.mint, currentBucket: -1}
	newIterator.iter = newRawChunkIterator(item, log)

	return &newIterator
}

// Advance the iterator to the specified chunk and time
func (it *bucketedRawChunkIterator) Seek(t int64) bool {
	return it.iter.Seek(t)
}

// Move to the next iterator item
func (it *bucketedRawChunkIterator) Next() bool {
	if it.step != 0 {
		it.currentBucket++
		return it.iter.Seek(it.mint + it.step*int64(it.currentBucket))
	} else {
		return it.iter.Next()
	}
}

// Read the time and value at the current location
func (it *bucketedRawChunkIterator) At() (int64, float64) {
	if it.step != 0 {
		prevT, prevV := it.iter.(*rawChunkIterator).PeakBack()
		nextT, nextV := it.iter.At()
		if nextT-it.currentBucketTime() > it.step {
			it.currentBucket = int((nextT - it.mint) / it.step)
		}
		interpolatedT, interpolatedV := GetInterpolateFunc(interpolateNext)(prevT, nextT, it.currentBucketTime(), prevV, nextV)
		return interpolatedT, interpolatedV
	} else {
		return it.iter.At()
	}
}

func (it *bucketedRawChunkIterator) currentBucketTime() int64 {
	return it.mint + it.step*int64(it.currentBucket)
}

func (it *bucketedRawChunkIterator) Err() error { return it.iter.Err() }

func (it *bucketedRawChunkIterator) AddChunks(results *qryResults) {
	it.iter.(*rawChunkIterator).AddChunks(*results)
}
