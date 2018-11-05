package pquerier

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"reflect"
)

type frameIterator struct {
	ctx         *selectQueryContext
	setIndex    int
	seriesIndex int
	columnNum   int
	err         error
}

// create new frame set iterator, frame iter has a SeriesSet interface (for Prometheus) plus columnar interfaces
func NewFrameIterator(ctx *selectQueryContext) *frameIterator {
	return &frameIterator{ctx: ctx, columnNum: ctx.totalColumns, setIndex: 0, seriesIndex: -1}
}

// advance to the next data frame
func (fi *frameIterator) NextSet() bool {
	fi.setIndex++
	return fi.setIndex-1 < len(fi.ctx.frameList)
}

// get current data frame
func (fi *frameIterator) GetFrame() *dataFrame {
	return fi.ctx.frameList[fi.setIndex]
}

// advance to the next time series (for Prometheus mode)
func (fi *frameIterator) Next() bool {

	// can advance series within a frame
	if fi.seriesIndex < fi.columnNum-1 {
		fi.seriesIndex++
		if fi.isCurrentSeriesHidden() {
			return fi.Next()
		}
		return true
	}

	// already in the last column in the last frame
	if fi.setIndex+1 >= len(fi.ctx.frameList) {
		return false
	}

	fi.setIndex++
	fi.seriesIndex = 0
	if fi.isCurrentSeriesHidden() {
		return fi.Next()
	}
	return true
}

// get current time series (for Prometheus mode)
func (fi *frameIterator) At() Series {
	s, err := fi.ctx.frameList[fi.setIndex].TimeSeries(fi.seriesIndex)
	if err != nil {
		fi.err = err
	}
	return s
}

func (fi *frameIterator) isCurrentSeriesHidden() bool {
	if fi.ctx.isRawQuery() {
		return false
	}
	col, err := fi.ctx.frameList[fi.setIndex].ColumnAt(fi.seriesIndex)
	if err != nil {
		fi.err = err
	}

	return col.GetColumnSpec().isHidden
}

func (fi *frameIterator) Err() error {
	return fi.err
}

// data frame, holds multiple value columns and an index (time) column
func NewDataFrame(columnsSpec []columnMeta, indexColumn Column, lset utils.Labels, hash uint64, isRawQuery, isAllColumnWildcard bool) *dataFrame {
	df := &dataFrame{lset: lset, hash: hash, isRawSeries: isRawQuery}
	// is raw query
	if isRawQuery {
		df.byName = make(map[string]int, 100)
	} else {
		numOfColumns := len(columnsSpec)
		df.index = indexColumn
		df.byName = make(map[string]int, numOfColumns)
		df.columns = make([]Column, 0, numOfColumns)
		df.aggregates = make(map[string]*aggregate.RawAggregatedSeries, numOfColumns)
		df.metricToCountColumn = map[string]Column{}
		if !isAllColumnWildcard {
			for i, col := range columnsSpec {
				df.columns = append(df.columns, &dataColumn{name: col.getColumnName(), spec: col})
				df.byName[col.getColumnName()] = i
			}
		}
	}

	return df
}

type dataFrame struct {
	lset utils.Labels
	hash uint64

	isRawSeries bool
	rawColumns  []Series

	columns []Column
	index   Column
	byName  map[string]int // name -> index in columns

	aggregates          map[string]*aggregate.RawAggregatedSeries // metric to aggregates
	metricToCountColumn map[string]Column
}

func (d *dataFrame) CalculateColumns() {
	// TODO - Create AggregatedColumn and use it instead of copying
	for _, col := range d.columns {
		aggrData := d.aggregates[col.GetColumnSpec().metric].GetAggregate(col.GetColumnSpec().function)
		col.SetData(aggrData, len(aggrData))
		if aggregate.IsCountAggregate(col.GetColumnSpec().function) {
			d.metricToCountColumn[col.GetColumnSpec().metric] = col
		}
	}
}

func (d *dataFrame) Len() int {
	if d.isRawSeries {
		return len(d.rawColumns)
	}
	return len(d.columns)
}

func (d *dataFrame) Labels() utils.Labels {
	return d.lset
}

func (d *dataFrame) Names() []string {
	names := make([]string, d.Len())

	for i := 0; i < d.Len(); i++ {
		names[i] = d.columns[i].Name()
	}

	return names
}

func (d *dataFrame) ColumnAt(i int) (Column, error) {
	if i >= d.Len() {
		return nil, fmt.Errorf("index %d out of bounds [0:%d]", i, d.Len())
	}
	return d.columns[i], nil
}

func (d *dataFrame) Columns() []Column {
	return d.columns
}

func (d *dataFrame) Column(name string) (Column, error) {
	i, ok := d.byName[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	return d.columns[i], nil
}

func (d *dataFrame) Index() Column {
	return d.index
}

func (d *dataFrame) TimeSeries(i int) (Series, error) {
	if d.isRawSeries {
		return d.rawColumns[i], nil
	} else {
		currentColumn, err := d.ColumnAt(i)
		if err != nil {
			return nil, err
		}
		return NewDataFrameColumnSeries(d.index,
			currentColumn,
			d.metricToCountColumn[currentColumn.GetColumnSpec().metric],
			d.Labels(),
			d.hash), nil
	}
}

// Column object, store a single value or index column/array
// There can be data columns or calculated columns (e.g. Avg built from count & sum columns)

// Column is a data column
type Column interface {
	Len() int                       // Number of elements
	Name() string                   // Column name
	DType() DType                   // Data type (e.g. IntType, FloatType ...)
	FloatAt(i int) (float64, error) // Float value at index i
	StringAt(i int) (string, error) // String value at index i
	TimeAt(i int) (int64, error)    // time value at index i
	GetColumnSpec() columnMeta      // Get the column's metadata
	SetData(interface{}, int)
}

// DType is data type
type DType reflect.Type

type dataColumn struct {
	name string
	size int
	data interface{}
	spec columnMeta
}

// Name returns the column name
func (dc *dataColumn) Name() string {
	return dc.name
}

// Len returns the number of elements
func (dc *dataColumn) Len() int {
	return dc.size
}

// DType returns the data type
func (dc *dataColumn) DType() DType {
	return reflect.TypeOf(dc.data)
}

// FloatAt returns float64 value at index i
func (dc *dataColumn) FloatAt(i int) (float64, error) {
	if !dc.isValidIndex(i) {
		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, dc.size)
	}

	typedCol, ok := dc.data.([]float64)
	if !ok {
		return 0, fmt.Errorf("wrong type (type is %s)", dc.DType())
	}

	return typedCol[i], nil
}

// StringAt returns string value at index i
func (dc *dataColumn) StringAt(i int) (string, error) {
	if !dc.isValidIndex(i) {
		return "", fmt.Errorf("index %d out of bounds [0:%d]", i, dc.size)
	}

	typedCol, ok := dc.data.([]string)
	if !ok {
		return "", fmt.Errorf("wrong type (type is %s)", dc.DType())
	}

	return typedCol[i], nil
}

// TimeAt returns time.Time value at index i
func (dc *dataColumn) TimeAt(i int) (int64, error) {
	if !dc.isValidIndex(i) {
		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, dc.size)
	}

	typedCol, ok := dc.data.([]int64)
	if !ok {
		return 0, fmt.Errorf("wrong type (type is %s)", dc.DType())
	}

	return typedCol[i], nil
}
func (dc *dataColumn) isValidIndex(i int) bool { return i >= 0 && i < dc.Len() }

func (dc *dataColumn) GetColumnSpec() columnMeta { return dc.spec }

func (dc *dataColumn) SetData(d interface{}, size int) {
	dc.data = d
	dc.size = size
}
