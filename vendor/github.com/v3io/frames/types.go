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

package frames

import (
	"time"

	"github.com/v3io/frames/pb"
)

// DType is data type
type DType pb.DType

// Possible data types
var (
	BoolType   = DType(pb.DType_BOOLEAN)
	FloatType  = DType(pb.DType_FLOAT)
	IntType    = DType(pb.DType_INTEGER)
	StringType = DType(pb.DType_STRING)
	TimeType   = DType(pb.DType_TIME)
)

// Column is a data column
type Column interface {
	Len() int                                 // Number of elements
	Name() string                             // Column name
	DType() DType                             // Data type (e.g. IntType, FloatType ...)
	Ints() ([]int64, error)                   // Data as []int64
	IntAt(i int) (int64, error)               // Int value at index i
	Floats() ([]float64, error)               // Data as []float64
	FloatAt(i int) (float64, error)           // Float value at index i
	Strings() []string                        // Data as []string
	StringAt(i int) (string, error)           // String value at index i
	Times() ([]time.Time, error)              // Data as []time.Time
	TimeAt(i int) (time.Time, error)          // time.Time value at index i
	Bools() ([]bool, error)                   // Data as []bool
	BoolAt(i int) (bool, error)               // bool value at index i
	Slice(start int, end int) (Column, error) // Slice of data
}

// Frame is a collection of columns
type Frame interface {
	Labels() map[string]interface{}          // Label set
	Names() []string                         // Column names
	Indices() []Column                       // Index columns
	Len() int                                // Number of rows
	Column(name string) (Column, error)      // Column by name
	Slice(start int, end int) (Frame, error) // Slice of Frame
	IterRows(includeIndex bool) RowIterator  // Iterate over rows
}

// RowIterator is an iterator over frame rows
type RowIterator interface {
	Next() bool                      // Advance to next row
	Row() map[string]interface{}     // Row as map of name->value
	RowNum() int                     // Current row number
	Indices() map[string]interface{} // MultiIndex as name->value
	Err() error                      // Iteration error
}

// DataBackend is an interface for read/write on backend
type DataBackend interface {
	// TODO: Expose name, type, config ... ?
	Read(request *ReadRequest) (FrameIterator, error)
	Write(request *WriteRequest) (FrameAppender, error) // TODO: use Appender for write streaming
	Create(request *CreateRequest) error
	Delete(request *DeleteRequest) error
	Exec(request *ExecRequest) (Frame, error)
}

// FrameIterator iterates over frames
type FrameIterator interface {
	Next() bool
	Err() error
	At() Frame
}

// FrameAppender appends frames
type FrameAppender interface {
	Add(frame Frame) error
	WaitForComplete(timeout time.Duration) error
}

// ReadRequest is a read/query request
type ReadRequest = pb.ReadRequest

// JoinStruct is a join structure
type JoinStruct = pb.JoinStruct

// WriteRequest is request for writing data
// TODO: Unite with probouf (currenly the protobuf message combines both this
// and a frame message)
type WriteRequest struct {
	Session *Session
	Backend string // backend name
	Table   string // Table name (path)
	// Data message sent with the write request (in case of a stream multiple messages can follow)
	ImmidiateData Frame
	// Expression template, for update expressions generated from combining columns data with expression
	Expression string
	// Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool
}

// CreateRequest is a table creation request
type CreateRequest = pb.CreateRequest

// DeleteRequest is a deletion request
type DeleteRequest = pb.DeleteRequest

// TableSchema is a table schema
type TableSchema = pb.TableSchema

// SchemaField represents a schema field for Avro record.
type SchemaField = pb.SchemaField

// SchemaKey is a schema key
type SchemaKey = pb.SchemaKey

// Session information
type Session = pb.Session

// Shortcut for fail/ignore
const (
	IgnoreError = pb.ErrorOptions_IGNORE
	FailOnError = pb.ErrorOptions_FAIL
)

// ExecRequest is execution request
type ExecRequest = pb.ExecRequest
