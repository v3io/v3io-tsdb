package pquerier

import (
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type frameIterator struct {
	ctx         *selectQueryContext
	setIndex    int
	seriesIndex int
	columnNum   int
	err         error
}

// create new frame set iterator, frame iter has a SeriesSet interface (for Prometheus) plus columnar interfaces
func NewFrameIterator(ctx *selectQueryContext) (*frameIterator, error) {
	if !ctx.isRawQuery() {
		for _, f := range ctx.frameList {
			if err := f.finishAllColumns(); err != nil {
				return nil, errors.Wrapf(err, "failed to create columns for DF=%v", f.Labels())
			}
		}
	}

	return &frameIterator{ctx: ctx, columnNum: ctx.totalColumns, setIndex: 0, seriesIndex: -1}, nil
}

// advance to the next data frame
func (fi *frameIterator) NextFrame() bool {
	fi.setIndex++
	return fi.setIndex-1 < len(fi.ctx.frameList)
}

// get current data frame
func (fi *frameIterator) GetFrame() (frames.Frame, error) {
	return fi.ctx.frameList[fi.setIndex-1].GetFrame()
}

// advance to the next time series (for Prometheus mode)
func (fi *frameIterator) Next() bool {

	var numberOfColumnsInCurrentSeries int
	if len(fi.ctx.frameList) > 0 {
		numberOfColumnsInCurrentSeries = len(fi.ctx.frameList[fi.setIndex].columnByName)
	}

	// can advance series within a frame
	if fi.seriesIndex < numberOfColumnsInCurrentSeries-1 {
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
func (fi *frameIterator) At() utils.Series {
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
func NewDataFrame(columnsSpec []columnMeta, indexColumn Column, lset utils.Labels, hash uint64, isRawQuery bool, columnSize int, useServerAggregates, showAggregateLabel bool) (*dataFrame, error) {
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
		df.nonEmptyRowsIndicators = make([]bool, columnSize)

		i := 0
		for _, col := range columnsSpec {
			// In case user wanted all metrics, save the template for every metric.
			// Once we know what metrics we have we will create Columns out of the column Templates
			if col.isWildcard() {
				df.columnsTemplates = append(df.columnsTemplates, col)
			} else {
				column, err := createColumn(col, columnSize, useServerAggregates)
				if err != nil {
					return nil, err
				}
				if aggregate.IsCountAggregate(col.function) {
					df.metricToCountColumn[col.metric] = column
				}
				df.columns = append(df.columns, column)
				df.columnByName[col.getColumnName()] = i
				i++
			}
		}
		for _, col := range df.columns {
			if !col.GetColumnSpec().isConcrete() {
				fillDependantColumns(col, df)
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
		column = NewDataColumn(col.getColumnName(), col, columnSize, frames.FloatType)
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

	// Order of the dependent columns should be the same as `wantedAggregations`.
	for _, agg := range wantedAggregations {
		for _, col := range df.columns {
			if col.GetColumnSpec().metric == wantedColumn.GetColumnSpec().metric &&
				agg == col.GetColumnSpec().function {
				columns = append(columns, col)
			}
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
	rawColumns            []utils.Series

	columnsTemplates       []columnMeta
	columns                []Column
	index                  Column
	columnByName           map[string]int // name -> index in columns
	nonEmptyRowsIndicators []bool

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
	var newColumns []Column
	for _, col := range d.columnsTemplates {
		col.metric = metricName
		newCol, err := createColumn(col, columnSize, useServerAggregates)
		if err != nil {
			return err
		}

		// Make sure there is only 1 count column per metric.
		// Count is the only column we automatically add so in some cases we get multiple count columns in the templates.
		_, ok := d.metricToCountColumn[metricName]
		if !aggregate.IsCountAggregate(col.function) || !ok {
			newColumns = append(newColumns, newCol)
		}
		if aggregate.IsCountAggregate(col.function) && !ok {
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

func (d *dataFrame) setDataAt(columnName string, index int, value interface{}) error {
	colIndex, ok := d.columnByName[columnName]
	if !ok {
		return fmt.Errorf("no such column %v", columnName)
	}
	col := d.columns[colIndex]
	err := col.SetDataAt(index, value)
	if err == nil {
		d.nonEmptyRowsIndicators[index] = true
	}

	return err
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

func (d *dataFrame) TimeSeries(i int) (utils.Series, error) {
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

// Creates Frames.columns out of tsdb columns.
// First do all the concrete columns and then the virtual who are dependant on the concrete.
func (d *dataFrame) finishAllColumns() error {
	// Marking as deleted every index (row) that has no data.
	// Also, adding "blank" rows when needed to align all columns to the same time.
	// Iterating backwards to not miss any deleted cell.
	for i := len(d.nonEmptyRowsIndicators) - 1; i >= 0; i-- {
		hasData := d.nonEmptyRowsIndicators[i]
		if !hasData {
			for _, col := range d.columns {
				_ = col.Delete(i)
			}
			_ = d.index.Delete(i)
		} else {
			for _, col := range d.columns {
				switch col.(type) {
				case *ConcreteColumn, *dataColumn:
					value, err := col.getBuilder().At(i)
					if err != nil || value == nil {
						col.getBuilder().Set(i, math.NaN())
					}
				}
			}
		}
	}

	var columnSize int
	var err error
	for _, col := range d.columns {
		switch col.(type) {
		case *dataColumn:
			err = col.finish()
		case *ConcreteColumn:
			err = col.finish()
			if columnSize == 0 {
				columnSize = col.FramesColumn().Len()
			} else if columnSize != col.FramesColumn().Len() {
				return fmt.Errorf("column length mismatch %v!=%v col=%v", columnSize, col.FramesColumn().Len(), col.Name())
			}
		}
		if err != nil {
			return errors.Wrapf(err, "failed to create column '%v'", col.Name())
		}
	}
	for _, col := range d.columns {
		switch newCol := col.(type) {
		case *virtualColumn:
			newCol.size = columnSize
			err = col.finish()
		}
		if err != nil {
			return errors.Wrapf(err, "failed to create column '%v'", col.Name())
		}
	}

	err = d.index.finish()

	return err
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
	var timeData []time.Time

	columns := make([]frames.ColumnBuilder, len(d.rawColumns))
	nonExhaustedIterators := len(d.rawColumns)
	seriesToDataType := make([]frames.DType, len(d.rawColumns))
	seriesTodefaultValue := make([]interface{}, len(d.rawColumns))
	currentTime := int64(math.MaxInt64)
	nextTime := int64(math.MaxInt64)
	seriesHasMoreData := make([]bool, len(d.rawColumns))

	for i, rawSeries := range d.rawColumns {
		if rawSeries.Iterator().Next() {
			seriesHasMoreData[i] = true
			t, _ := rawSeries.Iterator().At()
			if t < nextTime {
				nextTime = t
			}
		} else {
			nonExhaustedIterators--
		}

		currentEnc := chunkenc.EncXOR
		if ser, ok := rawSeries.(*V3ioRawSeries); ok {
			currentEnc = ser.encoding
		}

		if currentEnc == chunkenc.EncVariant {
			columns[i] = frames.NewSliceColumnBuilder(rawSeries.Labels().Get(config.PrometheusMetricNameAttribute),
				frames.StringType, 0)
			seriesToDataType[i] = frames.StringType
			seriesTodefaultValue[i] = ""
		} else {
			columns[i] = frames.NewSliceColumnBuilder(rawSeries.Labels().Get(config.PrometheusMetricNameAttribute),
				frames.FloatType, 0)
			seriesToDataType[i] = frames.FloatType
			seriesTodefaultValue[i] = math.NaN()
		}
	}

	for nonExhaustedIterators > 0 {
		currentTime = nextTime
		nextTime = int64(math.MaxInt64)
		timeData = append(timeData, time.Unix(currentTime/1000, (currentTime%1000)*1e6))

		for seriesIndex, rawSeries := range d.rawColumns {
			iter := rawSeries.Iterator()

			var v interface{}
			var t int64

			if seriesToDataType[seriesIndex] == frames.StringType {
				t, v = iter.AtString()
			} else {
				t, v = iter.At()
			}

			if t == currentTime {
				columns[seriesIndex].Append(v)
				if iter.Next() {
					t, _ = iter.At()
				} else {
					nonExhaustedIterators--
					seriesHasMoreData[seriesIndex] = false
				}
			} else {
				columns[seriesIndex].Append(seriesTodefaultValue[seriesIndex])
			}

			if seriesHasMoreData[seriesIndex] && t < nextTime {
				nextTime = t
			}
		}
	}

	numberOfRows := len(timeData)
	colSpec := columnMeta{metric: "time"}
	d.index = NewDataColumn("time", colSpec, numberOfRows, frames.TimeType)
	d.index.SetData(timeData, numberOfRows)

	d.columns = make([]Column, len(d.rawColumns))
	for i, series := range d.rawColumns {
		name := series.Labels().Get(config.PrometheusMetricNameAttribute)
		spec := columnMeta{metric: name}
		col := NewDataColumn(name, spec, numberOfRows, seriesToDataType[i])
		col.framesCol = columns[i].Finish()
		d.columns[i] = col
	}

	d.isRawColumnsGenerated = true
}

func (d *dataFrame) shouldGenerateRawColumns() bool { return d.isRawSeries && !d.isRawColumnsGenerated }

func (d *dataFrame) GetFrame() (frames.Frame, error) {
	var framesColumns []frames.Column
	if d.shouldGenerateRawColumns() {
		d.rawSeriesToColumns()
	}
	for _, col := range d.columns {
		if !col.GetColumnSpec().isHidden {
			framesColumns = append(framesColumns, col.FramesColumn())
		}
	}

	return frames.NewFrame(framesColumns, []frames.Column{d.index.FramesColumn()}, d.Labels().Map())
}

// Column object, store a single value or index column/array
// There can be data columns or calculated columns (e.g. Avg built from count & sum columns)

// Column is a data column
type Column interface {
	Len() int                        // Number of elements
	Name() string                    // Column name
	DType() frames.DType             // Data type (e.g. IntType, FloatType ...)
	FloatAt(i int) (float64, error)  // Float value at index i
	StringAt(i int) (string, error)  // String value at index i
	TimeAt(i int) (time.Time, error) // time value at index i
	GetColumnSpec() columnMeta       // Get the column's metadata
	SetDataAt(i int, value interface{}) error
	SetData(d interface{}, size int) error
	GetInterpolationFunction() InterpolationFunction
	FramesColumn() frames.Column
	Delete(index int) error

	setMetricName(name string)
	getBuilder() frames.ColumnBuilder
	finish() error
}

type basicColumn struct {
	name                  string
	size                  int
	spec                  columnMeta
	interpolationFunction InterpolationFunction
	builder               frames.ColumnBuilder
	framesCol             frames.Column
}

func (c *basicColumn) getBuilder() frames.ColumnBuilder {
	return c.builder
}

func (c *basicColumn) finish() error {
	c.framesCol = c.builder.Finish()
	return nil
}

func (c *basicColumn) Delete(index int) error {
	return c.builder.Delete(index)
}

func (c *basicColumn) FramesColumn() frames.Column {
	return c.framesCol
}

// Name returns the column name
func (c *basicColumn) Name() string {
	return c.name
}

// Len returns the number of elements
func (c *basicColumn) Len() int {
	if c.framesCol != nil {
		return c.framesCol.Len()
	}
	return c.size
}

func (c *basicColumn) isValidIndex(i int) bool { return i >= 0 && i < c.size }

func (c *basicColumn) GetColumnSpec() columnMeta { return c.spec }

func (c *basicColumn) setMetricName(name string) {
	c.spec.metric = name
	c.name = c.spec.getColumnName()
}

func (c *basicColumn) SetDataAt(i int, value interface{}) error {
	if !c.isValidIndex(i) {
		return fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}
	return nil
}

func (c *basicColumn) SetData(d interface{}, size int) error {
	return errors.New("method not supported")
}
func (c *basicColumn) GetInterpolationFunction() InterpolationFunction {
	return c.interpolationFunction
}

func NewDataColumn(name string, colSpec columnMeta, size int, datatype frames.DType) *dataColumn {
	dc := &dataColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size,
		interpolationFunction: GetInterpolateFunc(colSpec.interpolationType, colSpec.interpolationTolerance),
		builder:               frames.NewSliceColumnBuilder(name, datatype, size)}}
	return dc

}

type dataColumn struct {
	basicColumn
}

// DType returns the data type
func (dc *dataColumn) DType() frames.DType {
	return dc.framesCol.DType()
}

// FloatAt returns float64 value at index i
func (dc *dataColumn) FloatAt(i int) (float64, error) {
	return dc.framesCol.FloatAt(i)
}

// StringAt returns string value at index i
func (dc *dataColumn) StringAt(i int) (string, error) {
	return dc.framesCol.StringAt(i)
}

// TimeAt returns time.Time value at index i
func (dc *dataColumn) TimeAt(i int) (time.Time, error) {
	return dc.framesCol.TimeAt(i)
}

func (dc *dataColumn) SetData(d interface{}, size int) error {
	dc.size = size
	var err error
	dc.framesCol, err = frames.NewSliceColumn(dc.name, d)
	return err
}

func (dc *dataColumn) SetDataAt(i int, value interface{}) error {
	if !dc.isValidIndex(i) {
		return fmt.Errorf("index %d out of bounds [0:%d]", i, dc.size)
	}

	var err error
	switch value.(type) {
	case float64:
		// Update requested cell, only if not trying to override an existing value with NaN
		prev, _ := dc.builder.At(i)
		if !(math.IsNaN(value.(float64)) && prev != nil && !math.IsNaN(prev.(float64))) {
			err = dc.builder.Set(i, value)
		}
	default:
		err = dc.builder.Set(i, value)
	}
	return err
}

func NewConcreteColumn(name string, colSpec columnMeta, size int, setFunc func(old, new interface{}) interface{}) *ConcreteColumn {
	col := &ConcreteColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size,
		interpolationFunction: GetInterpolateFunc(colSpec.interpolationType, colSpec.interpolationTolerance),
		builder:               frames.NewSliceColumnBuilder(name, frames.FloatType, size)}, setFunc: setFunc}
	return col
}

type ConcreteColumn struct {
	basicColumn
	setFunc func(old, new interface{}) interface{}
}

func (c *ConcreteColumn) DType() frames.DType {
	return c.framesCol.DType()
}
func (c *ConcreteColumn) FloatAt(i int) (float64, error) {
	return c.framesCol.FloatAt(i)
}
func (c *ConcreteColumn) StringAt(i int) (string, error) {
	return "", errors.New("aggregated column does not support string type")
}
func (c *ConcreteColumn) TimeAt(i int) (time.Time, error) {
	return time.Unix(0, 0), errors.New("aggregated column does not support time type")
}
func (c *ConcreteColumn) SetDataAt(i int, val interface{}) error {
	if !c.isValidIndex(i) {
		return fmt.Errorf("index %d out of bounds [0:%d]", i, c.size)
	}
	value, _ := c.builder.At(i)
	err := c.builder.Set(i, c.setFunc(value, val))
	return err
}

func NewVirtualColumn(name string, colSpec columnMeta, size int, function func([]Column, int) (interface{}, error)) Column {
	col := &virtualColumn{basicColumn: basicColumn{name: name, spec: colSpec, size: size,
		interpolationFunction: GetInterpolateFunc(colSpec.interpolationType, colSpec.interpolationTolerance),
		builder:               frames.NewSliceColumnBuilder(name, frames.FloatType, size)},
		function: function}
	return col
}

type virtualColumn struct {
	basicColumn
	dependantColumns []Column
	function         func([]Column, int) (interface{}, error)
}

func (c *virtualColumn) finish() error {
	data := make([]float64, c.Len())
	var err error
	for i := 0; i < c.Len(); i++ {
		value, err := c.function(c.dependantColumns, i)
		if err != nil {
			return err
		}
		data[i] = value.(float64)
	}

	c.framesCol, err = frames.NewSliceColumn(c.name, data)
	if err != nil {
		return err
	}
	return nil
}

func (c *virtualColumn) DType() frames.DType {
	return c.framesCol.DType()
}
func (c *virtualColumn) FloatAt(i int) (float64, error) {
	return c.framesCol.FloatAt(i)
}
func (c *virtualColumn) StringAt(i int) (string, error) {
	return c.framesCol.StringAt(i)
}
func (c *virtualColumn) TimeAt(i int) (time.Time, error) {
	return time.Unix(0, 0), errors.New("aggregated column does not support time type")
}
