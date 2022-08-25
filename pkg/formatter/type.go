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
package formatter

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const DefaultOutputFormat = "text"

func NewFormatter(format string, cfg *Config) (Formatter, error) {
	if cfg == nil {
		cfg = &Config{TimeFormat: time.RFC3339}
	}
	switch format {
	case "", DefaultOutputFormat:
		return textFormatter{baseFormatter{cfg: cfg}}, nil
	case "csv":
		return csvFormatter{baseFormatter{cfg: cfg}}, nil
	case "json":
		return simpleJSONFormatter{baseFormatter{cfg: cfg}}, nil
	case "none":
		return testFormatter{baseFormatter{cfg: cfg}}, nil

	default:
		return nil, fmt.Errorf("unknown formatter type %s", format)
	}
}

type Formatter interface {
	Write(out io.Writer, set utils.SeriesSet) error
}

type Config struct {
	TimeFormat string
}

type baseFormatter struct {
	cfg *Config
}

func labelsToStr(labels utils.Labels) (string, string) {
	name := ""
	var lbls []string
	for _, lbl := range labels {
		if lbl.Name == "__name__" {
			name = lbl.Value
		} else {
			lbls = append(lbls, lbl.Name+"="+lbl.Value)
		}
	}
	return name, strings.Join(lbls, ",")
}
