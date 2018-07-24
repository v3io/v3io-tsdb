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

const DefaultConfigurationFileName = "v3io.yaml"

type V3ioConfig struct {
	// V3IO Connection details: Url, Data container, relative path for this dataset, credentials
	V3ioUrl   string `json:"v3ioUrl"`
	Container string `json:"container"`
	Path      string `json:"path"`
	Username  string `json:"username"`
	Password  string `json:"password"`

	// Disable is use in Prometheus to disable v3io and work with the internal TSDB
	Disabled bool `json:"disabled,omitempty"`
	// Set logging level: debug | info | warn | error (info by default)
	Verbose string `json:"verbose,omitempty"`
	// Number of parallel V3IO worker routines
	Workers int `json:"workers"`
	// Number of parallel V3IO worker routines for queries (default is min between 8 and Workers)
	QryWorkers int `json:"qryWorkers"`
	// Max uncommitted (delayed) samples allowed per metric
	MaxBehind int `json:"maxBehind"`
	// Override last chunk (by default on restart it will append from the last point if possible)
	OverrideOld bool `json:"overrideOld"`
}

type DBPartConfig struct {
	// Indicating this is a valid Partition file, Signature == 'TSDB'
	Signature string `json:"signature"`
	// Version of the config
	Version string `json:"version"`
	// Description of this TSDB
	Description string `json:"description,omitempty"`
	// Partition Key, __name__ by default
	ShardingKey string `json:"partitionKey,omitempty"`
	// Sorting Key, dimensions used for sorting per DB shard
	SortingKey string `json:"sortingKey,omitempty"`
	// indicate if it is cyclic (single partition, return to first chunk after the last)
	IsCyclic bool `json:"isCyclic,omitempty"`
	// Number of hours per chunk (1hr default)
	HrInChunk int `json:"hrInChunk,omitempty"`
	// Days per table/object (in a partition), after N days will use a new table or go to start (Cyclic partition)
	// this is used only for the Head configuration, per partition we look at StartTime & EndTime
	DaysPerObj int `json:"daysPerObj,omitempty"`
	// How many days to save samples
	DaysRetention int `json:"daysRetention,omitempty"`
	// Start from time/date in Unix milisec
	StartTime int64 `json:"startTime,omitempty"`
	// End by time/date in Unix milisec
	EndTime int64 `json:"endTime,omitempty"`
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
	Rollups       string `json:"rollups,omitempty"`
	RollupMin     int    `json:"rollupMin,omitempty"`
	DelRawSamples bool   `json:"delRawSamples,omitempty"`
	// Dimensions to pre aggregate (vertical aggregation)
	PreAggragate []string `json:"preAggragate,omitempty"`
}

// TODO: add alerts config (name, match expr, for, lables, annotations)

func LoadConfig(path string) (*V3ioConfig, error) {

	envpath := os.Getenv("V3IO_TSDBCFG_PATH")
	if envpath != "" {
		path = envpath
	}

	if path == "" {
		path = DefaultConfigurationFileName
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return LoadFromData(data)
}

func LoadFromData(data []byte) (*V3ioConfig, error) {
	cfg := V3ioConfig{}
	err := yaml.Unmarshal(data, &cfg)

	InitDefaults(&cfg)

	return &cfg, err
}

func InitDefaults(cfg *V3ioConfig) {
	// Initialize default number of workers
	if cfg.Workers == 0 {
		cfg.Workers = 8
	}

	// init default number Query workers if not set to Min(8,Workers)
	if cfg.QryWorkers == 0 {
		if cfg.Workers < 8 {
			cfg.QryWorkers = cfg.Workers
		} else {
			cfg.QryWorkers = 8
		}
	}
}
