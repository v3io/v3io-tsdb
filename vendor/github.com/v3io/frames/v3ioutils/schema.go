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

package v3ioutils

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	v3io "github.com/v3io/v3io-go-http"

	"github.com/v3io/frames"
	"strings"
)

const (
	longType   = "long"
	doubleType = "double"
	stringType = "string"
	timeType   = "time"
	// TODO: boolType?
)

// NewSchema returns a new schema
func NewSchema(key string) V3ioSchema {
	return &OldV3ioSchema{Fields: []OldSchemaField{}, Key: key}
}

// SchemaFromJSON return a schema from JSON data
func SchemaFromJSON(data []byte) (V3ioSchema, error) {
	var schema OldV3ioSchema
	err := json.Unmarshal(data, &schema)
	return &schema, err
}

// V3ioSchema is schema for v3io
type V3ioSchema interface {
	AddColumn(name string, col frames.Column, nullable bool) error
	AddField(name string, val interface{}, nullable bool) error
	UpdateSchema(container *v3io.Container, tablePath string, newSchema V3ioSchema) error
}

// OldV3ioSchema is old v3io schema
type OldV3ioSchema struct {
	Fields           []OldSchemaField `json:"fields"`
	Key              string           `json:"key"`
	HashingBucketNum int              `json:"hashingBucketNum"`
}

// OldSchemaField is OldV3ioSchema field
type OldSchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable,omitempty"`
}

// AddColumn adds a column
func (s *OldV3ioSchema) AddColumn(name string, col frames.Column, nullable bool) error {
	var ftype string
	switch col.DType() {
	case frames.IntType:
		ftype = longType
	case frames.FloatType:
		ftype = doubleType
	case frames.StringType:
		ftype = stringType
	case frames.TimeType:
		ftype = timeType
	}

	field := OldSchemaField{Name: name, Type: ftype, Nullable: nullable}
	s.Fields = append(s.Fields, field)
	return nil
}

// AddField adds a field
func (s *OldV3ioSchema) AddField(name string, val interface{}, nullable bool) error {
	var ftype string
	switch val.(type) {
	case int, int32, int64:
		ftype = longType
	case float32, float64:
		ftype = doubleType
	case string:
		ftype = stringType
	case time.Time:
		ftype = timeType
	}

	field := OldSchemaField{Name: name, Type: ftype, Nullable: nullable}
	s.Fields = append(s.Fields, field)
	return nil
}

// toJSON retrun JSON representation of schema
func (s *OldV3ioSchema) toJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *OldV3ioSchema) merge(new *OldV3ioSchema) (bool, error) {
	changed := false
	for _, field := range new.Fields {
		index := -1
		for j := 0; j < len(s.Fields); j++ {
			if s.Fields[j].Name == field.Name {
				index = j
			}
		}

		if index >= 0 && field.Type != s.Fields[index].Type {
			if field.Type == stringType {
				s.Fields[index].Type = stringType
				changed = true
				continue
			}

			if field.Type == doubleType && s.Fields[index].Type == longType {
				s.Fields[index].Type = doubleType
				changed = true
				continue
			}

			if field.Type == timeType || s.Fields[index].Type == timeType {
				return changed, fmt.Errorf(
					"Schema change from %s to %s is not allowed", s.Fields[index].Type, field.Type)
			}
		} else {
			s.Fields = append(s.Fields, field)
			changed = true
		}
	}

	if s.Key != new.Key && new.Key != "" {
		s.Key = new.Key
		changed = true
	}

	return changed, nil
}

// UpdateSchema updates the schema
func (s *OldV3ioSchema) UpdateSchema(container *v3io.Container, tablePath string, newSchema V3ioSchema) error {
	changed, err := s.merge(newSchema.(*OldV3ioSchema))
	if err != nil {
		return errors.Wrap(err, "failed to merge schema")
	}

	if changed {
		body, err := s.toJSON()
		if err != nil {
			return errors.Wrap(err, "failed to marshal schema")
		}
		err = container.Sync.PutObject(&v3io.PutObjectInput{
			Path: tablePath + ".%23schema", Body: body})
		if err != nil {
			if strings.Contains(err.Error(), "status 401") {
				return errors.New("unauthorized update (401), may be caused by wrong password or credentials")
			}

			return errors.Wrap(err, "failed to update schema")
		}
	}

	return nil
}
