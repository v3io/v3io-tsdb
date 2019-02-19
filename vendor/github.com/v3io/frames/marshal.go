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

// We encode messages in protobuf with an int64 leading size header

package frames

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/v3io/frames/pb"
)

var (
	byteOrder = binary.LittleEndian
)

// Encoder is message encoder
type Encoder struct {
	w io.Writer
}

// NewEncoder returns new Encoder
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w}
}

// MarshalFrame serializes a frame to []byte
func MarshalFrame(frame Frame) ([]byte, error) {
	iface, ok := frame.(pb.Framed)
	if !ok {
		return nil, fmt.Errorf("unknown frame type")
	}

	return proto.Marshal(iface.Proto())
}

// UnmarshalFrame de-serialize a frame from []byte
func UnmarshalFrame(data []byte) (Frame, error) {
	msg := &pb.Frame{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	return NewFrameFromProto(msg), nil
}

// Encode encoders the message to e.w
func (e *Encoder) Encode(msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "can't encode with message")
	}

	size := int64(len(data))
	if err := binary.Write(e.w, byteOrder, size); err != nil {
		return errors.Wrap(err, "can't write size header")
	}

	n, err := e.w.Write(data)
	if err != nil {
		return errors.Wrap(err, "can't write message body")
	}

	if int64(n) != size {
		return errors.Errorf("wrote only %d bytes out of %d", n, size)
	}

	return nil
}

// Decoder is message decoder
type Decoder struct {
	r   io.Reader
	buf *bytes.Buffer
}

// NewDecoder returns a new Decoder
func NewDecoder(r io.Reader) *Decoder {
	var buf bytes.Buffer
	return &Decoder{r, &buf}
}

// Decode decodes message from d.r
func (d *Decoder) Decode(msg proto.Message) error {
	var size int64
	if err := binary.Read(d.r, byteOrder, &size); err != nil {
		if err == io.EOF {
			// Propogate EOF to clients
			return err
		}
		return errors.Wrap(err, "can't read header")
	}

	d.buf.Reset()
	n, err := io.CopyN(d.buf, d.r, size)
	if err != nil {
		return errors.Wrap(err, "can't read message body")
	}

	if n != size {
		return errors.Errorf("read only %d bytes out of %d", n, size)
	}

	if err := proto.Unmarshal(d.buf.Bytes(), msg); err != nil {
		return errors.Wrap(err, "can't decode message body")
	}

	return nil
}
