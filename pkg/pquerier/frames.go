package pquerier

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"reflect"
	"time"
)

// Possible data types
var (
	IntType    DType = reflect.TypeOf([]int64{})
	FloatType  DType = reflect.TypeOf([]float64{})
	StringType DType = reflect.TypeOf([]string{})
	TimeType   DType = reflect.TypeOf([]time.Time{})
	BoolType   DType = reflect.TypeOf([]bool{})
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
func NewDataFrame(columnsSpec []columnMeta, indexColumn Column, lset utils.Labels, hash uint64, isRawQuery, isAllColumnWildcard bool, columnSize int, useServerAggregates bool) (*dataFrame, error) {
	df := &dataFrame{lset: lset, hash: hash, isRawSeries: isRawQuery}
	// is raw query
	if isRawQuery {
		df.columnByName = make(map[string]int, 100)
	} else {
		numOfColumns := len(columnsSpec)
		df.index = indexColumn
		df.columnByName = make(map[string]int, numOfColumns)
		df.columns = make([]Column, 0, numOfColumns)
		df.metricToCountColumn = map[string]Column{}
		if !isAllColumnWildcard {
			for i, col := range columnsSpec {
				var column Column
				if col.function != 0 {
					if col.isConcrete() {
						function, err := getAggreagteFunction(col.function, useServerAggregates)
						if err != nil {
							return nil, err
						}
						column = NewConcreteColumn(col.getColumnName(), col, columnSize, function)
						if aggregate.IsCountAggregate(col.function) {
							df.metricToCountColumn[col.metric] = column
						}
					} else {
						function, err := getVirtualColumnFunction(col.function)
						if err != nil {
							return nil, err
						}

						column = NewVirtualColumn(col.getColumnName(), col, columnSize, function)
					}
				} else {
					column = NewDataColumn(col.getColumnName(), col, columnSize, FloatType)
				}
				df.columns = append(df.columns, column)
				df.columnByName[col.getColumnName()] = i
			}

			for _, col := range df.columns {
				if !col.GetColumnSpec().isConcrete() {
					fillDependantColumns(col, df)
				}
			}
		}
	}

	return df, nil
}

func getAggreagteFunction(aggrType aggregate.AggrType, useServerAggregates bool) (func(interface{}, interface{}) interface{}, error) {
	if useServerAggregates {
		return aggregate.GetServerAggregationsFunction(aggrType)
	} else {
		return aggregate.GetClientAggregationsFunction(aggrType)
	}
}

func fillDependantColumns(wantedColumn Column, df *dataFrame) {
	wantedAggregations := aggregate.GetDependantAggregates(wantedColumn.GetColumnSpec().function)
	var columns []Column
	for _, col := range df.columns {
		if col.GetColumnSpec().metric == wantedColumn.GetColumnSpec().metric &&
			aggregate.ContainsAggregate(wantedAggregations, col.GetColumnSpec().function) {
			columns = append(columns, col)
		}
	}
	wantedColumn.(*virtualColumn).dependantColumns = columns
}

func getVirtualColumnFunction(aggrType aggregate.AggrType) (func([]Column, int) (interface{}, error), error) {
	function, err := aggregate.GetServerVirtualAggregationFunction(aggrType)
	if err != nil {
		return nil, err
	}
	return func(columns []Column, index int) (interface{}, error) {
		data := make([]float64, len(columns))
		for i, c := range columns {
			v, err := c.FloatAt(index)
			if err != nil {
				return nil, err
			}

			data[i] = v
		}
		return function(data), nil
	}, nil
}

type dataFrame struct {
	lset utils.Labels
	hash uint64

	isRawSeries bool
	rawColumns  []Series

	columns      []Column
	index        Column
	columnByName map[string]int // name -> index in columns

	metricToCountColumn map[string]Column
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
	i, ok := d.columnByName[name]
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
	SetDataAt(i int, value interface{}) error
	GetInterpolationFunction() (InterpolationFunction, int64)
}

type basicColumn struct {
	name string
	size int
	spec columnMeta
}

// Name returns the column name
func (c *basicColumn) Name() string {
	return c.name
}

// Len returns the number of elements
func (c *basicColumn) Len() int {
	return c.size
}

func (c *basicColumn) isValidIndex(i int) bool { return i >= 0 && i < c.size }

func (c *basicColumn) GetColumnSpec() columnMeta { return c.spec }

func (c *basicColumn) SetDataAt(i int, value interface{}) error { return nil }

// DType is data type
type DType reflect.Type

func NewDataColumn(name string, colSpec columnMeta, size int, datatype DType) *dataColumn {
	dc := &dataColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size}}
	dc.initializeData(datatype)
	dc.interpolationFunction = GetInterpolateFunc(colSpec.interpolationType)
	dc.interpolationTolerance = colSpec.interpolationTolerance
	return dc

}

type dataColumn struct {
	basicColumn
	data                   interface{}
	interpolationFunction  InterpolationFunction
	interpolationTolerance int64
}

func (dc *dataColumn) GetInterpolationFunction() (InterpolationFunction, int64) {
	return dc.interpolationFunction, dc.interpolationTolerance
}

func (dc *dataColumn) initializeData(dataType DType) {
	switch dataType {
	case IntType:
		dc.data = make([]int64, dc.size)
	case FloatType:
		dc.data = make([]float64, dc.size)
	case StringType:
		dc.data = make([]string, dc.size)
	case TimeType:
		dc.data = make([]time.Time, dc.size)
	case BoolType:
		dc.data = make([]bool, dc.size)
	}
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

func (dc *dataColumn) SetData(d interface{}, size int) {
	dc.data = d
	dc.size = size
}

func (dc *dataColumn) SetDataAt(i int, value interface{}) error {
	if !dc.isValidIndex(i) {
		return fmt.Errorf("index %d out of bounds [0:%d]", i, dc.size)
	}

	switch reflect.TypeOf(dc.data) {
	case IntType:
		dc.data.([]int64)[i] = value.(int64)
	case FloatType:
		dc.data.([]float64)[i] = value.(float64)
	case StringType:
		dc.data.([]string)[i] = value.(string)
	case TimeType:
		dc.data.([]time.Time)[i] = value.(time.Time)
	case BoolType:
		dc.data.([]bool)[i] = value.(bool)
	}
	return nil
}

func NewConcreteColumn(name string, colSpec columnMeta, size int, setFunc func(old, new interface{}) interface{}) *ConcreteColumn {
	col := &ConcreteColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size}, setFunc: setFunc}
	col.data = make([]interface{}, size)
	return col
}

type ConcreteColumn struct {
	basicColumn
	setFunc func(old, new interface{}) interface{}
	data    []interface{}
}

func (c *ConcreteColumn) DType() DType {
	var a float64
	return reflect.TypeOf(a)
}
func (c *ConcreteColumn) FloatAt(i int) (float64, error) {
	if !c.isValidIndex(i) {
		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}
	if c.data[i] == nil {
		return math.NaN(), nil
	}
	return c.data[i].(float64), nil
}
func (c *ConcreteColumn) StringAt(i int) (string, error) {
	return "", errors.New("aggregated column does not support string type")
}
func (c *ConcreteColumn) TimeAt(i int) (int64, error) {
	return 0, errors.New("aggregated column does not support time type")
}
func (c *ConcreteColumn) SetDataAt(i int, val interface{}) error {
	if !c.isValidIndex(i) {
		return fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}

	c.data[i] = c.setFunc(c.data[i], val)
	return nil
}
func (c *ConcreteColumn) GetInterpolationFunction() (InterpolationFunction, int64) { return nil, 0 }

func NewVirtualColumn(name string, colSpec columnMeta, size int, function func([]Column, int) (interface{}, error)) Column {
	col := &virtualColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size}, function: function}
	return col
}

type virtualColumn struct {
	basicColumn
	dependantColumns []Column
	function         func([]Column, int) (interface{}, error)
}

func (c *virtualColumn) DType() DType {
	return nil
}
func (c *virtualColumn) FloatAt(i int) (float64, error) {
	if !c.isValidIndex(i) {
		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}

	value, err := c.function(c.dependantColumns, i)

	if err != nil {
		return 0, err
	}
	return value.(float64), nil
}
func (c *virtualColumn) StringAt(i int) (string, error) {
	if !c.isValidIndex(i) {
		return "", fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}

	value, err := c.function(c.dependantColumns, i)
	if err != nil {
		return "", err
	}
	return value.(string), nil
}
func (c *virtualColumn) TimeAt(i int) (int64, error) {
	if !c.isValidIndex(i) {
		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}

	value, err := c.function(c.dependantColumns, i)
	if err != nil {
		return 0, err
	}
	return value.(int64), nil
}
func (c *virtualColumn) GetInterpolationFunction() (InterpolationFunction, int64) { return nil, 0 }

//func NewVirtualRawColumn(name string, colSpec columnMeta, size int, function func([]Column, int) (interface{}, error)) Column {
//	col := &virtualColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size}, function: function}
//	return col
//}
//
//type virtualRawColumn struct {
//	basicColumn
//	iter SeriesIterator
//}
//
//func (c *virtualRawColumn) DType() DType {
//	//c.iter.
//	return nil
//}
//func (c *virtualRawColumn) FloatAt(i int) (float64, error) {
//	if !c.isValidIndex(i) {
//		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
//	}
//
//	value, err := c.function(c.dependantColumns, i)
//
//	if err != nil {
//		return 0, err
//	}
//	return value.(float64), nil
//}
//func (c *virtualRawColumn) StringAt(i int) (string, error) {
//	if !c.isValidIndex(i) {
//		return "", fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
//	}
//
//	value, err := c.function(c.dependantColumns, i)
//	if err != nil {
//		return "", err
//	}
//	return value.(string), nil
//}
//func (c *virtualRawColumn) TimeAt(i int) (int64, error) {
//	if !c.isValidIndex(i) {
//		return 0, fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
//	}
//
//	value, err := c.function(c.dependantColumns, i)
//	if err != nil {
//		return 0, err
//	}
//	return value.(int64), nil
//}
//func (c *virtualRawColumn) GetInterpolationFunction() InterpolationFunction { return nil }
