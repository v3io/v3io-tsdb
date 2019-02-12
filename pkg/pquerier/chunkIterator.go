package pquerier

import (
	"strings"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// Chunk-list series iterator
type RawChunkIterator struct {
	mint, maxt int64

	chunks   []chunkenc.Chunk
	encoding chunkenc.Encoding

	chunkIndex int
	chunksMax  []int64
	iter       chunkenc.Iterator
	log        logger.Logger

	prevT int64
	prevV float64
}

func newRawChunkIterator(queryResult *qryResults, log logger.Logger) utils.SeriesIterator {
	maxt := queryResult.query.maxt
	maxTime := queryResult.fields[config.MaxTimeAttrName]
	if maxTime != nil && int64(maxTime.(int)) < maxt {
		maxt = int64(maxTime.(int))
	}

	newIterator := RawChunkIterator{
		mint: queryResult.query.mint, maxt: maxt, log: log.GetChild("rawChunkIterator"), encoding: queryResult.encoding}

	newIterator.AddChunks(queryResult)

	if len(newIterator.chunks) == 0 {
		// If there's no data, create a null iterator
		return &utils.NullSeriesIterator{}
	} else {
		newIterator.iter = newIterator.chunks[0].Iterator()
		return &newIterator
	}
}

// Advance the iterator to the specified chunk and time
func (it *RawChunkIterator) Seek(t int64) bool {

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
		it.updatePrevPoint()
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

			// Free up memory of old chunk
			it.chunks[it.chunkIndex] = nil

			it.chunkIndex++
			it.iter = it.chunks[it.chunkIndex].Iterator()
		}
	}
}

func (it *RawChunkIterator) updatePrevPoint() {
	t, v := it.At()
	if !(t == 0 && v == 0) {
		it.prevT, it.prevV = t, v
	}
}

// Move to the next iterator item
func (it *RawChunkIterator) Next() bool {
	it.updatePrevPoint()
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

	// Free up memory of old chunk
	it.chunks[it.chunkIndex] = nil

	it.chunkIndex++
	it.iter = it.chunks[it.chunkIndex].Iterator()
	return it.Next()
}

// Read the time and value at the current location
func (it *RawChunkIterator) At() (t int64, v float64) { return it.iter.At() }

func (it *RawChunkIterator) AtString() (t int64, v string) { return it.iter.AtString() }

func (it *RawChunkIterator) Err() error { return it.iter.Err() }

func (it *RawChunkIterator) Encoding() chunkenc.Encoding { return it.encoding }

func (it *RawChunkIterator) AddChunks(item *qryResults) {
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

		// In case we get both raw chunks and server aggregates, only go over the chunks.
		if !strings.Contains(attr, config.AggregateAttrPrefix) {
			values := item.fields[attr]
			if values != nil {
				bytes := values.([]byte)

				chunk, err := chunkenc.FromData(it.log, it.encoding, bytes, 0)
				if err != nil {
					it.log.ErrorWith("Error reading chunk buffer", "columns", item.query.attrs, "err", err)
				} else {
					chunks = append(chunks, chunk)
					// Calculate the end time for the current chunk
					chunksMax = append(chunksMax,
						firstChunkTime+int64(i+1)*item.query.partition.TimePerChunk()-1)
				}
			}
			i++
		}
	}

	it.chunks = append(it.chunks, chunks...)
	it.chunksMax = append(it.chunksMax, chunksMax...)
}

func (it *RawChunkIterator) PeakBack() (t int64, v float64) { return it.prevT, it.prevV }

func NewRawSeries(results *qryResults, logger logger.Logger) (utils.Series, error) {
	newSeries := V3ioRawSeries{fields: results.fields, logger: logger, encoding: results.encoding}
	err := newSeries.initLabels()
	if err != nil {
		return nil, err
	}
	newSeries.iter = newRawChunkIterator(results, logger)
	return &newSeries, nil
}

type V3ioRawSeries struct {
	fields   map[string]interface{}
	lset     utils.Labels
	iter     utils.SeriesIterator
	logger   logger.Logger
	hash     uint64
	encoding chunkenc.Encoding
}

func (s *V3ioRawSeries) Labels() utils.Labels { return s.lset }

// Get the unique series key for sorting
func (s *V3ioRawSeries) GetKey() uint64 {
	if s.hash == 0 {
		s.hash = s.lset.Hash()
	}
	return s.hash
}

func (s *V3ioRawSeries) Iterator() utils.SeriesIterator { return s.iter }

func (s *V3ioRawSeries) AddChunks(results *qryResults) {
	switch iter := s.iter.(type) {
	case *RawChunkIterator:
		iter.AddChunks(results)
	case utils.NullSeriesIterator:
		s.iter = newRawChunkIterator(results, s.logger)
	}
}

// Initialize the label set from _lset and _name attributes
func (s *V3ioRawSeries) initLabels() error {
	name, ok := s.fields[config.MetricNameAttrName].(string)
	if !ok {
		return errors.Errorf("error in initLabels; bad metric name: %v", s.fields[config.MetricNameAttrName].(string))
	}
	lsetAttr, ok := s.fields[config.LabelSetAttrName].(string)
	if !ok {
		return errors.Errorf("error in initLabels; bad labels set: %v", s.fields[config.LabelSetAttrName].(string))
	}

	lset, err := utils.LabelsFromStringWithName(name, lsetAttr)

	if err != nil {
		return errors.Errorf("error in initLabels; failed to parse labels set string: %v. err: %v", s.fields[config.LabelSetAttrName].(string), err)
	}

	s.lset = lset
	return nil
}
