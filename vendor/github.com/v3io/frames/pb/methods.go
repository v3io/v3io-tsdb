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

package pb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	intRe          = regexp.MustCompile("^[0-9]+$")
	passwdRe       = regexp.MustCompile("password:\"[^\"]*\"")
	passwdMask     = "*****"
	passwdMaskFull = fmt.Sprintf("password:\"%s\"", passwdMask)
	sessionFields  []string
)

// Framed is a frame based on protobuf
type Framed interface {
	Proto() *Frame
}

// GoValue return value as interface{}
func (v *Value) GoValue() (interface{}, error) {
	switch v.GetValue().(type) {
	case *Value_Ival:
		return v.GetIval(), nil
	case *Value_Fval:
		return v.GetFval(), nil
	case *Value_Sval:
		return v.GetSval(), nil
	case *Value_Tval:
		return NSToTime(v.GetTval()), nil
	case *Value_Bval:
		return v.GetBval(), nil
	}

	return nil, fmt.Errorf("unknown Value type - %T", v.GetValue())
}

// SetValue sets the value from Go type
func (v *Value) SetValue(i interface{}) error {
	switch i.(type) {
	case bool:
		v.Value = &Value_Bval{Bval: i.(bool)}
	case float64: // JSON encodes numbers as floats
		v.Value = &Value_Fval{Fval: i.(float64)}
	case int, int64, int32, int16, int8:
		ival, ok := AsInt64(i)
		if !ok {
			return fmt.Errorf("unsupported type for %T - %T", v, i)
		}
		v.Value = &Value_Ival{Ival: ival}
	case string:
		v.Value = &Value_Sval{Sval: i.(string)}
	case time.Time:
		t := i.(time.Time)
		v.Value = &Value_Tval{Tval: t.UnixNano()}
	default:
		return fmt.Errorf("unsupported type for %T - %T", v, i)
	}

	return nil
}

// Attributes return the attibutes
// TODO: Calculate once (how to add field in generate protobuf code?)
func (r *CreateRequest) Attributes() map[string]interface{} {
	return AsGoMap(r.AttributeMap)
}

// SetAttribute sets an attribute
func (r *CreateRequest) SetAttribute(key string, value interface{}) error {
	if r.AttributeMap == nil {
		r.AttributeMap = make(map[string]*Value)
	}

	pbVal := &Value{}
	if err := pbVal.SetValue(value); err != nil {
		return err
	}

	r.AttributeMap[key] = pbVal
	return nil
}

// NSToTime returns time from epoch nanoseconds
func NSToTime(ns int64) time.Time {
	return time.Unix(ns/1e9, ns%1e9)
}

// AsGoMap returns map with interface{} values
func AsGoMap(mv map[string]*Value) map[string]interface{} {
	m := make(map[string]interface{})
	for key, value := range mv {
		m[key], _ = value.GoValue()
	}

	return m
}

// FromGoMap return map[string]*Value
func FromGoMap(m map[string]interface{}) (map[string]*Value, error) {
	out := make(map[string]*Value)
	for key, val := range m {
		pbVal := &Value{}
		if err := pbVal.SetValue(val); err != nil {
			return nil, errors.Wrapf(err, "can't encode %s", key)
		}
		out[key] = pbVal
	}

	return out, nil
}

// MarshalJSON marshal Value as JSON object
func (v *Value) MarshalJSON() ([]byte, error) {
	val, err := v.GoValue()
	if err != nil {
		return nil, err
	}
	return json.Marshal(val)
}

// UnmarshalJSON will unmarshal encoded native Go type to value
// (Implement json.Unmarshaler interface)
func (v *Value) UnmarshalJSON(data []byte) error {
	var i interface{}
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}

	// JSON encodes numbers as floats
	f, ok := i.(float64)
	if ok && intRe.Match(data) {
		i = int64(f)
	}

	// TODO: Time
	return v.SetValue(i)
}

// Property return a schema property
func (s *SchemaField) Property(key string) (interface{}, bool) {
	if s.Properties == nil {
		return nil, false
	}

	val, ok := s.Properties[key]
	if !ok {
		return nil, false
	}

	v, err := val.GoValue()
	if err != nil {
		return nil, false
	}

	return v, true
}

// Format implements fmt.Formatter
// We're doing this to hide passwords from prints
func (s *Session) Format(state fmt.State, verb rune) {
	if s == nil {
		state.Write([]byte("<nil>"))
		return
	}

	switch verb {
	case 's', 'q':
		val := passwdRe.ReplaceAllString(s.String(), passwdMaskFull)
		if verb == 'q' {
			val = fmt.Sprintf("%q", val)
		}
		fmt.Fprint(state, val)
	case 'v':
		if state.Flag('#') {
			fmt.Fprintf(state, "%T", s)
		}
		fmt.Fprint(state, "{")
		val := reflect.ValueOf(*s)
		for i, name := range sessionFields {
			if state.Flag('#') || state.Flag('+') {
				fmt.Fprintf(state, "%s:", name)
			}
			fld := val.FieldByName(name)
			if name == "Password" && fld.Len() > 0 {
				fmt.Fprint(state, passwdMask)
			} else {
				fmt.Fprintf(state, "%v", fld)
			}

			if i < len(sessionFields)-1 {
				if state.Flag('#') || i > 0 {
					fmt.Fprint(state, " ")
				}
			}
		}
		fmt.Fprint(state, "}")
	}
}

func init() {
	typ := reflect.TypeOf(Session{})
	sessionFields = make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		// Ignore protobuf internal fields
		if strings.HasPrefix(field.Name, "XXX_") {
			continue
		}
		sessionFields = append(sessionFields, field.Name)
	}
	sort.Strings(sessionFields)
}

// AsInt64 convert val to int64, it'll return 0, false on failure
func AsInt64(val interface{}) (int64, bool) {
	switch val.(type) {
	case int64:
		return val.(int64), true
	case int:
		return int64(val.(int)), true
	case int32:
		return int64(val.(int32)), true
	case int16:
		return int64(val.(int16)), true
	case int8:
		return int64(val.(int8)), true
	}

	return 0, false
}

// Arg returns the value of argument
func (r *ExecRequest) Arg(name string) (interface{}, error) {
	return r.Args[name].GoValue()
}
