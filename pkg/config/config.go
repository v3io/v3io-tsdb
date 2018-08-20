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
const SCHEMA_CONFIG = ".schema"

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
	// Default timeout duration in Seconds (if not set, 1 Hour timeout will be used )
	DefaultTimeout int `json:"timeout,omitempty"`
	// The size of batch to use during ingestion
	BatchSize int `json:"batchSize,omitempty"`
	// Size of sample in bytes for worst an best compression scenarios
	MinimumSampleSize int `json:"minimumSampleSize,omitempty"`
	MaximumSampleSize int `json:"maximumSampleSize,omitempty"`
	// Max size of a partition object
	MaximumPartitionSize int `json:"maximumPartitionSize,omitempty"`
	// Size of chunk in bytes for worst an best compression scenarios
	MinimumChunkSize int `json:"minimumChunkSize,omitempty"`
	MaximumChunkSize int `json:"maximumChunkSize,omitempty"`
}

type Rollup struct {
	Aggregators            []string `json:"aggregators"`
	AggregatorsGranularity string   `json:"aggregatorsGranularity"`
	//["cloud","local"] for the aggregators and sample chucks
	StorageClass string `json:"storageClass"`
	//in hours. 0  means no need to save samples
	SampleRetention int `json:"sampleRetention"`
	// format : 1m, 7d, 3h . Possible intervals: m/d/h
	LayerRetentionTime string `json:"layerRetentionTime"`
}

type TableSchema struct {
	Version             int      `json:"version"`
	RollupLayers        []Rollup `json:"rollupLayers"`
	ShardingBuckets     int      `json:"shardingBuckets"`
	PartitionerInterval string   `json:"partitionerInterval"`
	ChunckerInterval    string   `json:"chunckerInterval"`
}

type PartitionSchema struct {
	Version                int      `json:"version"`
	Aggregators            []string `json:"aggregators"`
	AggregatorsGranularity string   `json:"aggregatorsGranularity"`
	StorageClass           string   `json:"storageClass"`
	SampleRetention        int      `json:"sampleRetention"`
	PartitionerInterval    string   `json:"partitionerInterval"`
	ChunckerInterval       string   `json:"chunckerInterval"`
}

type Partition struct {
	StartTime  int64           `json:"startTime"`
	SchemaInfo PartitionSchema `json:"schemaInfo"`
}

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Items    string `json:"items,omitempty"`
}

type Schema struct {
	TableSchemaInfo     TableSchema     `json:"tableSchemaInfo"`
	PartitionSchemaInfo PartitionSchema `json:"partitionSchemaInfo"`
	Partitions          []Partition     `json:"partitions"`
	Fields              []SchemaField   `json:"fields"`
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

	// init default batch size
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
}
