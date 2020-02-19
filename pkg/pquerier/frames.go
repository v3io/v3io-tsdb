package pquerier

import (
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
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
func newFrameIterator(ctx *selectQueryContext) (*frameIterator, error) {
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

	if fi.seriesIndex < numberOfColumnsInCurrentSeries-1 {
		// can advance series within a frame
		fi.seriesIndex++
	} else if fi.setIndex+1 >= len(fi.ctx.frameList) {
		// already in the last column in the last frame
		return false
	} else {
		// advance to next frame
		fi.setIndex++
		fi.seriesIndex = 0
	}

	if fi.isCurrentSeriesHidden() {
		return fi.Next()
	}

	series := fi.ctx.frameList[fi.setIndex]
	// If raw series is nil
	if series.isRawSeries && series.rawColumns[fi.seriesIndex] == nil {
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
func newDataFrame(columnsSpec []columnMeta, indexColumn Column, lset utils.Labels, hash uint64, isRawQuery bool, columnSize int, useServerAggregates, showAggregateLabel bool) (*dataFrame, error) {
	df := &dataFrame{lset: lset, hash: hash, isRawSeries: isRawQuery, showAggregateLabel: showAggregateLabel}
	// is raw query
	if isRawQuery {
		df.columnByName = make(map[string]int, len(columnsSpec))

		// Create the columns in the DF based on the requested columns order.
		for i, col := range columnsSpec {
			if col.metric == "" {
				df.isWildcardSelect = true
				break
			}
			df.columnByName[col.getColumnName()] = i
		}

		// If no specific order was requested (like when querying for all metrics),
		// discard order and reset columns for future initialization.
		if df.isWildcardSelect {
			df.columnByName = make(map[string]int, len(columnsSpec))
			df.rawColumns = []utils.Series{}
		} else {
			// Initialize `rawcolumns` to the requested size.
			df.rawColumns = make([]utils.Series, len(columnsSpec))
		}
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
		column = newDataColumn(col.getColumnName(), col, columnSize, frames.FloatType)
	}

	return column, nil
}

func getAggreagteFunction(aggrType aggregate.AggrType, useServerAggregates bool) (func(interface{}, interface{}) interface{}, error) {
	if useServerAggregates {
		return aggregate.GetServerAggregationsFunction(aggrType)
	}
	return aggregate.GetClientAggregationsFunction(aggrType)
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
	nullValuesMaps         []*pb.NullValuesMap

	metrics             map[string]struct{}
	metricToCountColumn map[string]Column

	isWildcardSelect bool
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
		err := d.rawSeriesToColumns()
		if err != nil {
			return nil, err
		}
	}
	return d.columns[i], nil
}

func (d *dataFrame) Columns() ([]Column, error) {
	if d.shouldGenerateRawColumns() {
		err := d.rawSeriesToColumns()
		if err != nil {
			return nil, err
		}
	}
	return d.columns, nil
}

func (d *dataFrame) Column(name string) (Column, error) {
	if d.shouldGenerateRawColumns() {
		err := d.rawSeriesToColumns()
		if err != nil {
			return nil, err
		}
	}
	i, ok := d.columnByName[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	return d.columns[i], nil
}

func (d *dataFrame) Index() (Column, error) {
	if d.shouldGenerateRawColumns() {
		err := d.rawSeriesToColumns()
		if err != nil {
			return nil, err
		}
	}
	return d.index, nil
}

func (d *dataFrame) TimeSeries(i int) (utils.Series, error) {
	if d.isRawSeries {
		return d.rawColumns[i], nil
	}
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
						err := col.getBuilder().Set(i, math.NaN())
						if err != nil {
							return errors.Wrap(err, fmt.Sprintf("could not create new column at index %d", i))
						}
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
func (d *dataFrame) rawSeriesToColumns() error {
	var timeData []time.Time
	var currentTime int64
	numberOfRawColumns := len(d.rawColumns)
	columns := make([]frames.ColumnBuilder, numberOfRawColumns)
	nonExhaustedIterators := numberOfRawColumns
	seriesToDataType := make([]frames.DType, numberOfRawColumns)
	seriesToDefaultValue := make([]interface{}, numberOfRawColumns)
	nextTime := int64(math.MaxInt64)
	seriesHasMoreData := make([]bool, numberOfRawColumns)
	emptyMetrics := make(map[int]string)

	d.nullValuesMaps = make([]*pb.NullValuesMap, 0)
	nullValuesRowIndex := -1

	for i, rawSeries := range d.rawColumns {
		if rawSeries == nil {
			missingColumn := "(unknown column)"
			for columnName, index := range d.columnByName {
				if index == i {
					missingColumn = columnName
					break
				}
			}
			emptyMetrics[i] = missingColumn
			nonExhaustedIterators--
			continue
		}
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
			seriesToDefaultValue[i] = ""
		} else {
			columns[i] = frames.NewSliceColumnBuilder(rawSeries.Labels().Get(config.PrometheusMetricNameAttribute),
				frames.FloatType, 0)
			seriesToDataType[i] = frames.FloatType
			seriesToDefaultValue[i] = math.NaN()
		}
	}

	for nonExhaustedIterators > 0 {
		currentTime = nextTime
		nextTime = int64(math.MaxInt64)
		timeData = append(timeData, time.Unix(currentTime/1000, (currentTime%1000)*1e6))

		// add new row to null values map
		d.nullValuesMaps = append(d.nullValuesMaps, &pb.NullValuesMap{NullColumns: make(map[string]bool)})
		nullValuesRowIndex++

		for seriesIndex, rawSeries := range d.rawColumns {
			if rawSeries == nil {
				continue
			}
			iter := rawSeries.Iterator()

			var v interface{}
			var t int64

			if seriesToDataType[seriesIndex] == frames.StringType {
				t, v = iter.AtString()
			} else {
				t, v = iter.At()
			}

			if t == currentTime {
				e := columns[seriesIndex].Append(v)
				if e != nil {
					return errors.Wrap(e, fmt.Sprintf("could not append value %v", v))
				}
				if iter.Next() {
					t, _ = iter.At()
				} else {
					nonExhaustedIterators--
					seriesHasMoreData[seriesIndex] = false
				}
			} else {
				e := columns[seriesIndex].Append(seriesToDefaultValue[seriesIndex])
				if e != nil {
					return errors.Wrap(e, fmt.Sprintf("could not append from default value %v", seriesToDefaultValue[seriesIndex]))
				}
				d.nullValuesMaps[nullValuesRowIndex].NullColumns[columns[seriesIndex].Name()] = true
			}

			if seriesHasMoreData[seriesIndex] && t < nextTime {
				nextTime = t
			}
		}
	}

	numberOfRows := len(timeData)
	colSpec := columnMeta{metric: "time"}
	d.index = newDataColumn("time", colSpec, numberOfRows, frames.TimeType)
	e := d.index.SetData(timeData, numberOfRows)
	if e != nil {
		return errors.Wrap(e, fmt.Sprintf("could not set data, timeData=%v, numberOfRows=%v", timeData, numberOfRows))
	}

	d.columns = make([]Column, numberOfRawColumns)

	for i, series := range d.rawColumns {
		if series == nil {
			continue
		}

		name := series.Labels().Get(config.PrometheusMetricNameAttribute)
		spec := columnMeta{metric: name}
		col := newDataColumn(name, spec, numberOfRows, seriesToDataType[i])
		col.framesCol = columns[i].Finish()
		d.columns[i] = col
	}

	if len(emptyMetrics) > 0 {
		nullValues := make([]float64, numberOfRows)
		for i := 0; i < numberOfRows; i++ {
			nullValues[i] = math.NaN()
		}
		for index, metricName := range emptyMetrics {
			spec := columnMeta{metric: metricName}
			col := newDataColumn(metricName, spec, numberOfRows, frames.FloatType)
			framesCol, err := frames.NewSliceColumn(metricName, nullValues)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("could not create empty column '%v'", metricName))
			}
			col.framesCol = framesCol
			d.columns[index] = col

			// mark empty columns
			for i := 0; i < numberOfRows; i++ {
				d.nullValuesMaps[i].NullColumns[col.name] = true
			}
		}
	}

	d.isRawColumnsGenerated = true

	return nil
}

func (d *dataFrame) shouldGenerateRawColumns() bool { return d.isRawSeries && !d.isRawColumnsGenerated }

func (d *dataFrame) GetFrame() (frames.Frame, error) {
	var framesColumns []frames.Column
	if d.shouldGenerateRawColumns() {
		err := d.rawSeriesToColumns()
		if err != nil {
			return nil, err
		}
	}
	for _, col := range d.columns {
		if !col.GetColumnSpec().isHidden {
			framesColumns = append(framesColumns, col.FramesColumn())
		}
	}

	return frames.NewFrameWithNullValues(framesColumns, []frames.Column{d.index.FramesColumn()}, d.Labels().Map(), d.nullValuesMaps)
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

func newDataColumn(name string, colSpec columnMeta, size int, datatype frames.DType) *dataColumn {
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
