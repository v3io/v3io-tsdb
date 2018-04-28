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
	// V3IO Connection details: Url, Data container, relative path for this dataset, credentials
	V3ioUrl   string `json:"v3ioUrl"`
	Container string `json:"container"`
	Path      string `json:"path"`
	Username  string `json:"username"`
	Password  string `json:"password"`

	// Disable is use in Prometheus to disable v3io and work with the internal TSDB
	Disabled bool `json:"disabled,omitempty"`
	// True will turn on Debug mode
	Verbose bool `json:"verbose,omitempty"`
	// Number of parallel V3IO worker routines
	Workers int `json:"workers"`
	// Max uncommitted (delayed) samples allowed per metric
	MaxBehind int `json:"maxBehind"`
	// Override last chunk (by default on restart it will append from the last point if possible)
	OverrideOld bool `json:"overrideOld"`

	// Number of hours per chunk (1hr default)
	HrInChunk int `json:"hrInChunk,omitempty"`
	// Days per table/object (in a partition), after N days will use a new table or go to start (Cyclic partition)
	DaysPerObj int `json:"daysPerObj,omitempty"`
	// How many days to save samples
	DaysRetention int `json:"daysRetention,omitempty"`
	// Partition name format e.g. 'dd-mm-yy'
	PartFormat string `json:"partFormat,omitempty"`

	// Comma seperated list of default aggregation functions e.g. 'count,sum,avg,max'
	DefaultRollups string `json:"defaultRollups,omitempty"`
	// Number of minutes per aggregation bucket (aggregation interval)
	RollupMin int `json:"rollupMin,omitempty"`
	// If true, dont save raw samples/chunks, only aggregates
	DelRawSamples bool `json:"delRawSamples,omitempty"`

	// Metric specific policy
	MetricsConfig map[string]MetricConfig `json:"metricsConfig,omitempty"`
}

type MetricConfig struct {
	HrInChunk     int    `json:"hrInChunk,omitempty"`
	DaysPerObj    int    `json:"chunksInObj"`
	Rollups       string `json:"rollups,omitempty"`
	RollupMin     int    `json:"rollupMin,omitempty"`
	DelRawSamples bool   `json:"delRawSamples,omitempty"`
	// Dimensions to pre aggregate (vertical aggregation)
	PreAggragate []string `json:"preAggragate,omitempty"`
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

	initDefaults(&cfg)

	return &cfg, err
}

func initDefaults(cfg *TsdbConfig) {
	// Initialize defaults
	if cfg.Workers == 0 {
		cfg.Workers = 8
	}
	if cfg.DaysPerObj == 0 {
		cfg.DaysPerObj = 1
	}
	if cfg.HrInChunk == 0 {
		cfg.HrInChunk = 1
	}
	if cfg.Path == "" {
		cfg.Path = "metrics"
	}
}
