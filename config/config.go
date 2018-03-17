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

package config

import (
	"github.com/ghodss/yaml"
	"io/ioutil"
	"os"
)

type TsdbConfig struct {
	V3ioUrl   string `json:"v3ioUrl"`
	Container string `json:"container"`
	Path      string `json:"path"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Verbose   bool   `json:"verbose,omitempty"`
	Workers   int    `json:"workers"`
	ChanSize  int    `json:"chanSize"`
	MaxBehind int    `json:"maxBehind"`
	ArraySize int    `json:"arraySize,omitempty"`
	//HrInChunk      int                     `json:"hrInChunk,omitempty"`
	DaysPerObj     int                     `json:"daysPerObj,omitempty"`
	DaysRetention  int                     `json:"daysRetention,omitempty"`
	DefaultRollups string                  `json:"defaultRollups,omitempty"`
	RollupHrs      int                     `json:"rollupHrs,omitempty"`
	DelRawSamples  bool                    `json:"delRawSamples,omitempty"`
	MetricsConfig  map[string]MetricConfig `json:"metricsConfig,omitempty"`
}

type MetricConfig struct {
	ArraySize     int      `json:"arraySize"`
	HrInChunk     int      `json:"hrInChunk,omitempty"`
	DaysPerObj    int      `json:"chunksInObj"`
	Rollups       string   `json:"rollups,omitempty"`
	RollupHrs     int      `json:"rollupHrs,omitempty"`
	DelRawSamples bool     `json:"delRawSamples,omitempty"`
	PreAggragate  []string `json:"preAggragate,omitempty"`
}

// TODO: add alerts config (name, match expr, for, lables, annotations)

func LoadConfig(path string) (*TsdbConfig, error) {

	envpath := os.Getenv("V3IO-COLDB-CFG")
	if envpath != "" {
		path = envpath
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return LoadFromData(data)
}

func LoadFromData(data []byte) (*TsdbConfig, error) {
	cfg := TsdbConfig{}
	err := yaml.Unmarshal(data, &cfg)

	// Initialize defaults
	if cfg.Workers == 0 {
		cfg.Workers = 8
	}
	if cfg.ArraySize == 0 {
		cfg.ArraySize = 1024
	}
	if cfg.ChanSize == 0 {
		cfg.ChanSize = 2048
	}
	if cfg.DaysPerObj == 0 {
		cfg.DaysPerObj = 1
	}
	if cfg.Path == "" {
		cfg.Path = "metrics"
	}

	return &cfg, err
}
