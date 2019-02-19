/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/frames/backends/utils"
)

// Backend is CSV backend
type Backend struct {
	rootDir string
	logger  logger.Logger
}

// NewBackend returns a new CSV backend
func NewBackend(logger logger.Logger, config *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {
	backend := &Backend{
		rootDir: config.RootDir,
		logger:  logger.GetChild("csv"),
	}

	return backend, nil
}

// Create will create a table
func (b *Backend) Create(request *frames.CreateRequest) error {
	csvPath := b.csvPath(request.Table)
	// TODO: Overwrite?
	if fileExists(csvPath) {
		return fmt.Errorf("table %q already exists", request.Table)
	}

	file, err := os.Create(csvPath)
	if err != nil {
		return errors.Wrapf(err, "can't create table file")
	}

	defer file.Close()
	if request.Schema == nil || len(request.Schema.Fields) == 0 {
		return nil
	}

	numFields := len(request.Schema.Fields)
	names := make([]string, numFields)
	for i, field := range request.Schema.Fields {
		if field.Name == "" {
			return fmt.Errorf("field %d with no name", i)
		}

		names[i] = field.Name
	}

	csvWriter := csv.NewWriter(file)
	if err := csvWriter.Write(names); err != nil {
		return errors.Wrapf(err, "can't create header")
	}

	csvWriter.Flush()
	if err := file.Sync(); err != nil {
		return errors.Wrap(err, "can't flush csv file")
	}

	return nil
}

// Delete will delete a table
func (b *Backend) Delete(request *frames.DeleteRequest) error {
	csvPath := b.csvPath(request.Table)
	if request.IfMissing == frames.FailOnError && !fileExists(csvPath) {
		return fmt.Errorf("table %q doesn't exist", request.Table)
	}

	if err := os.Remove(csvPath); err != nil {
		return errors.Wrapf(err, "can't delete %q", request.Table)
	}

	return nil
}

// Read handles reading
func (b *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {
	file, err := os.Open(b.csvPath(request.Table))
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	columns, err := reader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "can't read header (columns)")
	}

	it := &FrameIterator{
		logger:      b.logger,
		path:        request.Table,
		reader:      reader,
		columnNames: columns,
		limit:       int(request.Limit),
		frameLimit:  int(request.MessageLimit),
	}

	return it, nil
}

// Write handles writing
func (b *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {
	// TODO: Append?
	file, err := os.Create(b.csvPath(request.Table))
	if err != nil {
		return nil, err
	}

	ca := &csvAppender{
		writer:    file,
		csvWriter: csv.NewWriter(file),
		logger:    b.logger,
	}

	if request.ImmidiateData != nil {
		if err := ca.Add(request.ImmidiateData); err != nil {
			return nil, errors.Wrap(err, "can't Add ImmidiateData")
		}
	}

	return ca, nil

}

func getInt(r *frames.ExecRequest, name string, defval int) int {
	ival, err := r.Arg(name)
	if err != nil {
		return defval
	}

	val, ok := ival.(int64)
	if !ok {
		return defval
	}

	return int(val)
}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	if strings.ToLower(request.Command) == "ping" {
		b.logger.Info("PONG")
		nRows, nCols := getInt(request, "rows", 37), getInt(request, "cols", 4)
		cols := make([]frames.Column, nCols)
		for c := 0; c < nCols; c++ {
			name := fmt.Sprintf("col-%d", c)
			bld := frames.NewSliceColumnBuilder(name, frames.IntType, nRows)
			for r := 0; r < nRows; r++ {
				if err := bld.Set(r, r*c); err != nil {
					b.logger.WarnWith("can't set column value", "name", name, "row", r)
				}
			}
			cols[c] = bld.Finish()
		}
		return frames.NewFrame(cols, nil, nil)
	}

	return nil, fmt.Errorf("CSV backend does not support %q exec command", request.Command)
}

func (b *Backend) csvPath(table string) string {
	return fmt.Sprintf("%s/%s", b.rootDir, table)
}

// FrameIterator iterates over CSV
type FrameIterator struct {
	logger      logger.Logger
	path        string
	reader      *csv.Reader
	frame       frames.Frame
	err         error
	columnNames []string
	nRows       int
	limit       int
	frameLimit  int
}

// Next reads the next frame, return true of succeeded
func (it *FrameIterator) Next() bool {
	rows, err := it.readNextRows()
	if err != nil {
		it.logger.ErrorWith("can't read rows", "error", err)
		it.err = err
		return false
	}

	if len(rows) == 0 {
		return false
	}

	it.frame, err = it.buildFrame(rows)
	if err != nil {
		it.logger.ErrorWith("can't build frame", "error", err)
		it.err = err
		return false
	}

	return true
}

// At return the current Frame
func (it *FrameIterator) At() frames.Frame {
	return it.frame
}

// Err returns the last error
func (it *FrameIterator) Err() error {
	return it.err
}

func (it *FrameIterator) readNextRows() ([][]string, error) {
	var rows [][]string
	for r := 0; it.inLimits(r); r, it.nRows = r+1, it.nRows+1 {
		row, err := it.reader.Read()
		if err != nil {
			if err == io.EOF {
				it.logger.DebugWith("EOF", "numRows", it.nRows)
				return rows, nil
			}

			return nil, err
		}

		if len(row) != len(it.columnNames) {
			err := fmt.Errorf("%s:%d num columns don't match headers (%d != %d)", it.path, it.nRows, len(row), len(it.columnNames))
			it.logger.ErrorWith("row size mismatch", "error", err, "row", it.nRows)
			return nil, err
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (it *FrameIterator) inLimits(frameRow int) bool {
	if it.limit > 0 && it.nRows >= it.limit {
		return false
	}

	if it.frameLimit > 0 && frameRow >= it.frameLimit {
		return false
	}

	return true
}

func (it *FrameIterator) buildFrame(rows [][]string) (frames.Frame, error) {
	columns := make([]frames.Column, len(it.columnNames))
	for c, colName := range it.columnNames {
		var (
			val0 = it.parseValue(rows[0][c])
			col  frames.Column
			data interface{}
			err  error
		)

		switch val0.(type) {
		case int64:
			typedData := make([]int64, len(rows))
			typedData[0] = val0.(int64)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(int64)
				if !ok {
					err := fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
					it.logger.ErrorWith("type mismatch", "error", err)
					return nil, err
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		case float64:
			typedData := make([]float64, len(rows))
			typedData[0] = val0.(float64)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(float64)
				if !ok {
					err := fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
					it.logger.ErrorWith("type mismatch", "error", err)
					return nil, err
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		case string:
			typedData := make([]string, len(rows))
			typedData[0] = val0.(string)
			for r, row := range rows[1:] {
				typedData[r+1] = row[c] // +1 since we start in first row
			}
			data = typedData
		case time.Time:
			typedData := make([]time.Time, len(rows))
			typedData[0] = val0.(time.Time)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(time.Time)
				if !ok {
					err := fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
					it.logger.ErrorWith("type mismatch", "error", err)
					return nil, err
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		case bool:
			typedData := make([]bool, len(rows))
			typedData[0] = val0.(bool)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(bool)
				if !ok {
					err := fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
					it.logger.ErrorWith("type mismatch", "error", err)
					return nil, err
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		default:
			return nil, fmt.Errorf("%s - unknown type %T", colName, val0)
		}

		col, err = frames.NewSliceColumn(colName, data)
		if err != nil {
			it.logger.ErrorWith("can't build column", "error", err, "column", colName)
			return nil, errors.Wrapf(err, "can't build column %s", colName)
		}

		columns[c] = col
	}

	return frames.NewFrame(columns, nil, nil)
}

func (it *FrameIterator) parseValue(value string) interface{} {
	// time/date formats
	timeFormats := []string{time.RFC3339, time.RFC3339Nano, "2006-01-02"}
	for _, format := range timeFormats {
		t, err := time.Parse(format, value)
		if err == nil {
			return t
		}
	}

	// bool
	switch strings.ToLower(value) {
	case "true":
		return true
	case "false":
		return false
	}

	// int
	i, err := strconv.Atoi(value)
	if err == nil {
		return int64(i)
	}

	// float
	f, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return f
	}

	// Leave as string
	return value
}

type csvAppender struct {
	logger        logger.Logger
	writer        io.Writer
	csvWriter     *csv.Writer
	headerWritten bool
}

func (ca *csvAppender) Add(frame frames.Frame) error {
	ca.logger.InfoWith("adding frame", "size", frame.Len())
	names := frame.Names()
	if !ca.headerWritten {
		if err := ca.csvWriter.Write(names); err != nil {
			ca.logger.ErrorWith("can't write header", "error", err)
			return errors.Wrap(err, "can't write header")
		}
		ca.headerWritten = true
	}

	for r := 0; r < frame.Len(); r++ {
		record := make([]string, len(names))
		for c, name := range names {
			col, err := frame.Column(name)
			if err != nil {
				ca.logger.ErrorWith("can't get column", "error", err)
				return errors.Wrap(err, "can't get column")
			}

			val, err := utils.ColAt(col, r)
			if err != nil {
				ca.logger.ErrorWith("can't get value", "error", err, "name", name, "row", r)
				return errors.Wrapf(err, "%s:%d can't get value", name, r)
			}

			record[c] = fmt.Sprintf("%v", val)
		}

		if err := ca.csvWriter.Write(record); err != nil {
			ca.logger.ErrorWith("can't write record", "error", err)
			return errors.Wrap(err, "can't write record")
		}
	}

	return nil
}

// File Sync
type syncer interface {
	Sync() error
}

// WaitForComplete wait for write completion
func (ca *csvAppender) WaitForComplete(timeout time.Duration) error {
	ca.csvWriter.Flush()
	if err := ca.csvWriter.Error(); err != nil {
		ca.logger.ErrorWith("csv Flush", "error", err)
		return err
	}

	if s, ok := ca.writer.(syncer); ok {
		return s.Sync()
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func init() {
	if err := backends.Register("csv", NewBackend); err != nil {
		panic(err)
	}
}
