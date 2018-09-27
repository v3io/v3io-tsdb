/*
Copyright 2017 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nutest

import (
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/pkg/errors"
	"time"
)

type TestEvent struct {
	nuclio.AbstractEvent

	Body               []byte
	ContentType        string
	id                 nuclio.ID
	emptyByteArray     []byte
	Headers            map[string]interface{}
	Path               string
	URL                string
	Method             string
	Time               time.Time
}

var ErrUnsupported = errors.New("Event does not support this interface")


func (te *TestEvent) GetContentType() string {
	return te.ContentType
}

func (te *TestEvent) GetBody() []byte {
	return te.Body
}

func (te *TestEvent) GetPath() string {
	return te.Path
}

func (te *TestEvent) GetURL() string {
	return te.URL
}

func (te *TestEvent) GetMethod() string {
	return te.Method
}

func (te *TestEvent) GetHeaders() map[string]interface{} {
	return te.Headers
}

func (te *TestEvent) GetHeader(key string) interface{} {
	return te.Headers[key]
}

func (te *TestEvent) GetHeaderByteSlice(key string) []byte {
	value, found := te.Headers[key]
	if !found {
		return nil
	}

	switch typedValue := value.(type) {
	case string:
		return []byte(typedValue)
	case []byte:
		return typedValue
	default:
		return nil
	}
}

func (te *TestEvent) GetHeaderString(key string) string {
	return string(te.GetHeaderByteSlice(key))
}

func (te *TestEvent) GetTimestamp() time.Time {
	return te.Time
}

