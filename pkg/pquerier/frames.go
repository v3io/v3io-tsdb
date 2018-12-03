package pquerier

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
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
func (fi *frameIterator) NextFrame() bool {
	fi.setIndex++
	return fi.setIndex-1 < len(fi.ctx.frameList)
}

// get current data frame
func (fi *frameIterator) GetFrame() *dataFrame {
	return fi.ctx.frameList[fi.setIndex-1]
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
func NewDataFrame(columnsSpec []columnMeta, indexColumn Column, lset utils.Labels, hash uint64, isRawQuery, getAllMetrics bool, columnSize int, useServerAggregates, showAggregateLabel bool) (*dataFrame, error) {
	df := &dataFrame{lset: lset, hash: hash, isRawSeries: isRawQuery, showAggregateLabel: showAggregateLabel}
	// is raw query
	if isRawQuery {
		df.columnByName = make(map[string]int, len(columnsSpec))
	} else {
		numOfColumns := len(columnsSpec)
		df.index = indexColumn
		df.columnByName = make(map[string]int, numOfColumns)
		df.columns = make([]Column, 0, numOfColumns)
		df.metricToCountColumn = map[string]Column{}
		df.metrics = map[string]struct{}{}
		// In case user wanted all metrics, save the template for every metric.
		// Once we know what metrics we have we will create Columns out of the column Templates
		if getAllMetrics {
			df.columnsTemplates = columnsSpec
		} else {
			for i, col := range columnsSpec {
				df.metrics[col.metric] = struct{}{}
				column, err := createColumn(col, columnSize, useServerAggregates)
				if err != nil {
					return nil, err
				}
				if aggregate.IsCountAggregate(col.function) {
					df.metricToCountColumn[col.metric] = column
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

func createColumn(col columnMeta, columnSize int, useServerAggregates bool) (Column, error) {
	var column Column
	if col.function != 0 {
		if col.isConcrete() {
			function, err := getAggreagteFunction(col.function, useServerAggregates)
			if err != nil {
				return nil, err
			}
			column = NewConcreteColumn(col.getColumnName(), col, columnSize, function)
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

	return column, nil
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
	lset               utils.Labels
	hash               uint64
	showAggregateLabel bool

	isRawSeries           bool
	isRawColumnsGenerated bool
	rawColumns            []Series

	columnsTemplates []columnMeta
	columns          []Column
	index            Column
	columnByName     map[string]int // name -> index in columns

	metrics             map[string]struct{}
	metricToCountColumn map[string]Column
}

func (d *dataFrame) addMetricIfNotExist(metricName string, columnSize int, useServerAggregates bool) error {
	if _, ok := d.metrics[metricName]; !ok {
		return d.addMetricFromTemplate(metricName, columnSize, useServerAggregates)
	}
	return nil
}

func (d *dataFrame) addMetricFromTemplate(metricName string, columnSize int, useServerAggregates bool) error {
	newColumns := make([]Column, len(d.columnsTemplates))
	for i, col := range d.columnsTemplates {
		newCol, err := createColumn(col, columnSize, useServerAggregates)
		if err != nil {
			return err
		}

		newCol.setMetricName(metricName)
		newColumns[i] = newCol
		if aggregate.IsCountAggregate(col.function) {
			d.metricToCountColumn[metricName] = newCol
		}
	}

	numberOfColumns := len(d.columns)
	d.columns = append(d.columns, newColumns...)
	for i, col := range newColumns {
		d.columnByName[col.GetColumnSpec().getColumnName()] = numberOfColumns + i
		if !col.GetColumnSpec().isConcrete() {
			fillDependantColumns(col, d)
		}
	}
	d.metrics[metricName] = struct{}{}
	return nil
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
	if d.shouldGenerateRawColumns() {
		d.rawSeriesToColumns()
	}
	return d.columns[i], nil
}

func (d *dataFrame) Columns() []Column {
	if d.shouldGenerateRawColumns() {
		d.rawSeriesToColumns()
	}
	return d.columns
}

func (d *dataFrame) Column(name string) (Column, error) {
	if d.shouldGenerateRawColumns() {
		d.rawSeriesToColumns()
	}
	i, ok := d.columnByName[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	return d.columns[i], nil
}

func (d *dataFrame) Index() Column {
	if d.shouldGenerateRawColumns() {
		d.rawSeriesToColumns()
	}
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
			d.hash,
			d.showAggregateLabel), nil
	}
}

// Normalizing the raw data of different metrics to one timeline with both metric's times.
//
// for example the following time series:
// metric1 - (t0,v0), (t2, v1)
// metric2 - (t1,v2), (t2, v3)
//
// will be converted to:
// time		metric1		metric2
//	t0		  v0		  NaN
//	t1		  NaN		  v2
//	t2		  v1		  v3
//
func (d *dataFrame) rawSeriesToColumns() {
	var timeData []int64
	columns := make([][]float64, len(d.rawColumns))
	nonExhaustedIterators := len(d.rawColumns)

	currentTime := int64(math.MaxInt64)
	nextTime := int64(math.MaxInt64)

	for _, rawSeries := range d.rawColumns {
		rawSeries.Iterator().Next()
		t, _ := rawSeries.Iterator().At()
		if t < nextTime {
			nextTime = t
		}
	}

	for nonExhaustedIterators > 0 {
		currentTime = nextTime
		nextTime = int64(math.MaxInt64)
		timeData = append(timeData, currentTime)

		for seriesIndex, rawSeries := range d.rawColumns {
			iter := rawSeries.Iterator()
			t, v := iter.At()
			if t == currentTime {
				columns[seriesIndex] = append(columns[seriesIndex], v)
				if iter.Next() {
					t, _ = iter.At()
				} else {
					nonExhaustedIterators--
				}
			} else if t > currentTime {
				columns[seriesIndex] = append(columns[seriesIndex], math.NaN())
			}

			if t < nextTime {
				nextTime = t
			}
		}
	}

	numberOfRows := len(timeData)
	colSpec := columnMeta{metric: "time"}
	d.index = NewDataColumn("time", colSpec, numberOfRows, IntType)
	d.index.SetData(timeData, numberOfRows)

	d.columns = make([]Column, len(d.rawColumns))
	for i, series := range d.rawColumns {
		name := series.Labels().Get(config.PrometheusMetricNameAttribute)
		spec := columnMeta{metric: name}
		col := NewDataColumn(name, spec, numberOfRows, FloatType)
		col.SetData(columns[i], numberOfRows)
		d.columns[i] = col
	}

	d.isRawColumnsGenerated = true
}

func (d *dataFrame) shouldGenerateRawColumns() bool { return d.isRawSeries && !d.isRawColumnsGenerated }

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
	SetData(d interface{}, size int) error
	GetInterpolationFunction() (InterpolationFunction, int64)
	setMetricName(name string)
}

type basicColumn struct {
	name                   string
	size                   int
	spec                   columnMeta
	interpolationFunction  InterpolationFunction
	interpolationTolerance int64
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

func (c *basicColumn) setMetricName(name string) {
	c.spec.metric = name
	c.name = c.spec.getColumnName()
}

func (c *basicColumn) SetDataAt(i int, value interface{}) error { return nil }
func (c *basicColumn) SetData(d interface{}, size int) error {
	return errors.New("method not supported")
}
func (dc *basicColumn) GetInterpolationFunction() (InterpolationFunction, int64) {
	return dc.interpolationFunction, dc.interpolationTolerance
}

// DType is data type
type DType reflect.Type

func NewDataColumn(name string, colSpec columnMeta, size int, datatype DType) *dataColumn {
	dc := &dataColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size,
		interpolationFunction:  GetInterpolateFunc(colSpec.interpolationType),
		interpolationTolerance: colSpec.interpolationTolerance}}
	dc.initializeData(datatype)
	return dc

}

type dataColumn struct {
	basicColumn
	data interface{}
}

func (dc *dataColumn) initializeData(dataType DType) {
	switch dataType {
	case IntType:
		dc.data = make([]int64, dc.size)
	case FloatType:
		dc.data = make([]float64, dc.size)
		floats := dc.data.([]float64)
		for i := 0; i < dc.size; i++ {
			floats[i] = math.NaN()
		}
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

func (dc *dataColumn) SetData(d interface{}, size int) error {
	dc.data = d
	dc.size = size
	return nil
}

func (dc *dataColumn) SetDataAt(i int, value interface{}) error {
	if !dc.isValidIndex(i) {
		return fmt.Errorf("index %d out of bounds [0:%d]", i, dc.size)
	}

	switch reflect.TypeOf(dc.data) {
	case IntType:
		dc.data.([]int64)[i] = value.(int64)
	case FloatType:
		// Update requested cell, only if not trying to override an existing value with NaN
		if !(math.IsNaN(value.(float64)) && !math.IsNaN(dc.data.([]float64)[i])) {
			dc.data.([]float64)[i] = value.(float64)
		}
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
	col := &ConcreteColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size,
		interpolationFunction: GetInterpolateFunc(interpolateNone)}, setFunc: setFunc}
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

func NewVirtualColumn(name string, colSpec columnMeta, size int, function func([]Column, int) (interface{}, error)) Column {
	col := &virtualColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size,
		interpolationFunction: GetInterpolateFunc(interpolateNone)},
		function: function}
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
