package querier

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-tsdb/chunkenc"
	"github.com/v3io/v3io-tsdb/utils"
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"math"
	"strings"
)

func NewSeries(partition *utils.ColDBPartition, ic *v3ioutil.V3ioItemsCursor, mint, maxt int64) series {
	newSeries := series{partition: partition}
	newSeries.getLables(ic)
	newSeries.getSeriesIter(ic, mint, maxt)
	return newSeries
}

type series struct {
	partition *utils.ColDBPartition
	lset      labels.Labels
	iter      SeriesIterator
}

func (s series) Labels() labels.Labels            { return s.lset }
func (s series) Iterator() storage.SeriesIterator { return s.iter }

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the given timestamp.
	// If there's no value exactly at t, it advances to the first value
	// after t.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

func (s series) getLables(ic *v3ioutil.V3ioItemsCursor) {
	name := ic.GetField("_name").(string)
	lsetAttr := ic.GetField("_lset").(string)
	lset := labels.Labels{labels.Label{Name: "__name__", Value: name}}

	splitLset := strings.Split(lsetAttr, ",")
	for _, label := range splitLset {
		kv := strings.Split(label, "=")
		if len(kv) > 1 {
			lset = append(lset, labels.Label{Name: kv[0], Value: kv[1]})
		}
	}

	s.lset = lset
}

func (s series) getSeriesIter(ic *v3ioutil.V3ioItemsCursor, mint, maxt int64) {

	newIterator := v3ioSeriesIterator{mint: mint, maxt: maxt}
	newIterator.chunks = []chunkenc.Chunk{}
	newIterator.chunkMaxTime = []int64{}

	attrs, ids := utils.Range2Attrs("v", 0, mint, maxt)
	metaAttr := ic.GetField("_meta_v")
	// TODO: handle nil
	metaArray := v3ioutil.AsInt64Array(metaAttr.([]byte))
	for i, attr := range attrs {
		values := ic.GetField(attr)
		if values != nil && len(values.([]byte)) >= 24 {
			chunkID := ids[i]
			bytes := values.([]byte)
			meta := metaArray[chunkID]
			chunk, _ := chunkenc.FromBuffer(meta, bytes[16:])
			// TODO: err handle

			//c, _ := chunkenc.FromData(chunkenc.EncXOR, bytes[24:], count)
			newIterator.chunks = append(newIterator.chunks, chunk)
			newIterator.chunkMaxTime = append(newIterator.chunkMaxTime, int64(chunkID+1)*3600*1000) // TODO: use method to calc max

		}

	}
	// TODO: if len>0 + err handle
	newIterator.iter = newIterator.chunks[0].Iterator()
	s.iter = &newIterator
}

type v3ioSeriesIterator struct {
	mint, maxt int64 // TBD per block
	err        error

	chunks       []chunkenc.Chunk
	chunkMaxTime []int64
	chunkIndex   int
	iter         chunkenc.Iterator
}

func (it *v3ioSeriesIterator) Seek(t int64) bool {

	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	// TODO: check if T < chunk start + max hours in chunk
	//for ; it.chunks[it.i].MaxTime < t; it.i++ {
	//	if it.i == len(it.chunks)-1 {
	//		return false
	//	}
	//}
	//	it.cur = it.chunks[it.i].Chunk.Iterator()

	// TODO: can optimize to skip to the right chunk

	for i, s := range it.chunks[it.chunkIndex:] {
		if i > 0 {
			it.iter = s.Iterator()
		}
		for it.iter.Next() {
			t0, _ := it.At()
			if t0 >= t {
				return true
			}
		}
		it.chunkIndex++
	}
	return false
}

func (it *v3ioSeriesIterator) Next() bool {
	if it.iter.Next() {
		t, _ := it.iter.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
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

func (it *v3ioSeriesIterator) At() (t int64, v float64) { return it.iter.At() }

func (it *v3ioSeriesIterator) Err() error { return it.iter.Err() }

func uintToTV(data uint64, curT int64, curV float64) (int64, float64) {
	v := float64(math.Float32frombits(uint32(data)))
	t := int64(data >> 32)
	return curT + t, curV + v
}
