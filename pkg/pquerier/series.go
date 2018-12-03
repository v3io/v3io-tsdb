package pquerier

import (
	"math"

	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

func NewDataFrameColumnSeries(indexColumn, dataColumn, countColumn Column, labels utils.Labels, hash uint64, showAggregateLabel bool) *DataFrameColumnSeries {
	// If we need to return the Aggregate label then add it, otherwise (for example in prometheus) return labels without it
	if showAggregateLabel {
		labels = append(labels, utils.LabelsFromStringList(aggregate.AggregateLabel, dataColumn.GetColumnSpec().function.String())...)
	}

	// The labels we get from the Dataframe are agnostic to the metric name, since there might be several metrics in one Dataframe
	labels = append(labels, utils.LabelsFromStringList(config.PrometheusMetricNameAttribute, dataColumn.GetColumnSpec().metric)...)
	s := &DataFrameColumnSeries{labels: labels, key: hash}
	s.iter = &dataFrameColumnSeriesIterator{indexColumn: indexColumn, dataColumn: dataColumn, countColumn: countColumn, currentIndex: -1}
	return s
}

// This series converts two columns into a series of time-value pairs
type DataFrameColumnSeries struct {
	labels utils.Labels
	key    uint64
	iter   SeriesIterator
}

func (s *DataFrameColumnSeries) Labels() utils.Labels {
	return s.labels
}
func (s *DataFrameColumnSeries) Iterator() SeriesIterator { return s.iter }
func (s *DataFrameColumnSeries) GetKey() uint64           { return s.key }

type dataFrameColumnSeriesIterator struct {
	dataColumn  Column
	indexColumn Column
	countColumn Column // Count Column is needed to filter out empty buckets

	currentIndex int
	err          error
}

func (it *dataFrameColumnSeriesIterator) Seek(seekT int64) bool {
	if it.currentIndex >= it.dataColumn.Len() {
		return false
	}
	t, _ := it.At()
	if t >= seekT {
		return true
	}

	for it.Next() {
		t, _ := it.At()
		if t >= seekT {
			return true
		}
	}

	return false
}

func (it *dataFrameColumnSeriesIterator) At() (int64, float64) {
	t, err := it.indexColumn.TimeAt(it.currentIndex)
	if err != nil {
		it.err = err
	}
	v, err := it.dataColumn.FloatAt(it.currentIndex)
	if err != nil {
		it.err = err
	}
	return t, v
}

func (it *dataFrameColumnSeriesIterator) Next() bool {
	if it.err != nil {
		return false
	}
	it.currentIndex = it.getNextValidCell(it.currentIndex)

	// It is enough to only check one of the columns since we assume they are both the same size
	return it.currentIndex < it.indexColumn.Len()
}

func (it *dataFrameColumnSeriesIterator) Err() error { return it.err }

func (it *dataFrameColumnSeriesIterator) getNextValidCell(from int) (nextIndex int) {
	for nextIndex = from + 1; nextIndex < it.dataColumn.Len() && !it.doesCellHasData(nextIndex); nextIndex++ {
	}
	return
}

func (it *dataFrameColumnSeriesIterator) doesCellHasData(cell int) bool {
	// In case we don't have a count column (for example while down sampling) check if there is a real value at `cell`
	if it.countColumn == nil {
		f, err := it.dataColumn.FloatAt(cell)
		if err != nil {
			it.err = err
			return false
		}
		return !math.IsNaN(f)
	}
	val, err := it.countColumn.FloatAt(cell)
	if err != nil {
		it.err = err
		return false
	}
	return val > 0
}
