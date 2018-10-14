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
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

const (
	V3ioConfigEnvironmentVariable = "V3IO_TSDB_CONFIG"
	DefaultConfigurationFileName  = "v3io-tsdb-config.yaml"
	SchemaConfigFileName          = ".schema"

	defaultNumberOfIngestWorkers = 1
	defaultNumberOfQueryWorkers  = 8
	defaultBatchSize             = 64
	defaultTimeoutInSeconds      = 24 * 60 * 60 // 24 hours

	defaultMaximumSampleSize    = 8       // bytes
	defaultMaximumPartitionSize = 1700000 // 1.7MB
	defaultMinimumChunkSize     = 200     // bytes
	defaultMaximumChunkSize     = 32000   // bytes

	DefaultShardingBucketsCount   = 8
	DefaultStorageClass           = "local"
	DefaultIngestionRate          = ""
	DefaultAggregates             = "" // no aggregates by default
	DefaultAggregationGranularity = "1h"
	DefaultLayerRetentionTime     = "1y"
	DefaultSampleRetentionTime    = 0
	DefaultLogLevel               = "info"
	DefaultVerboseLevel           = "debug"
)

var (
	instance *V3ioConfig
	once     sync.Once
	failure  error
)

func Error() error {
	return failure
}

type V3ioConfig struct {
	// V3IO TSDB connection information - web-gateway service endpoint,
	// TSDB data container, relative TSDB table path within the container, and
	// authentication credentials for the web-gateway service
	WebApiEndpoint string `json:"webApiEndpoint"`
	Container      string `json:"container"`
	TablePath      string `json:"tablePath"`
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`

	// Disabled = true disables the V3IO TSDB configuration in Prometheus and
	// enables the internal Prometheus TSDB instead
	Disabled bool `json:"disabled,omitempty"`
	// Log level - "debug" | "info" | "warn" | "error"
	LogLevel string `json:"logLevel,omitempty"`
	// Number of parallel V3IO worker routines
	Workers int `json:"workers"`
	// Number of parallel V3IO worker routines for queries;
	// default = the minimum value between 8 and Workers
	QryWorkers int `json:"qryWorkers"`
	// Max uncommitted (delayed) samples allowed per metric
	MaxBehind int `json:"maxBehind"`
	// Override last chunk; by default, an append from the last point is attempted upon restart
	OverrideOld bool `json:"overrideOld"`
	// Default timeout duration, in seconds; default = 3,600 seconds (1 hour)
	DefaultTimeoutInSeconds int `json:"timeout,omitempty"`
	// Size of the samples batch to use during ingestion
	BatchSize int `json:"batchSize,omitempty"`
	// Maximum sample size, in bytes (for the worst compression scenario)
	MaximumSampleSize int `json:"maximumSampleSize,omitempty"`
	// Maximum size of a partition object
	MaximumPartitionSize int `json:"maximumPartitionSize,omitempty"`
	// Minimum chunk size, in bytes (for the best compression scenario)
	MinimumChunkSize int `json:"minimumChunkSize,omitempty"`
	// Maximum chunk size, in bytes (for the worst compression scenario)
	MaximumChunkSize int `json:"maximumChunkSize,omitempty"`
	// Number of sharding buckets
	ShardingBucketsCount int `json:"shardingBucketsCount,omitempty"`
	// Metrics-reporter configuration
	MetricsReporter MetricsReporterConfig `json:"performance,omitempty"`
	// Don't aggregate from raw chunks, for use when working as a Prometheus
	// TSDB library
	DisableClientAggr bool `json:"disableClientAggr,omitempty"`
}

type MetricsReporterConfig struct {
	// Report on shutdown (Boolean)
	ReportOnShutdown bool `json:"reportOnShutdown,omitempty"`
	// Output destination - "stdout" or "stderr"
	Output string `json:"output"`
	// Report periodically (Boolean)
	ReportPeriodically bool `json:"reportPeriodically,omitempty"`
	// Interval between consequence reports (in seconds)
	RepotInterval int `json:"reportInterval"`
}

type Rollup struct {
	Aggregates             []string `json:"aggregates"`
	AggregationGranularity string   `json:"aggregationGranularity"`
	// Storage class for the aggregates and sample chunks - "cloud" | "local"
	StorageClass string `json:"storageClass"`
	// [FUTURE] Sample retention period, in hours. 0 means no need to save samples.
	SampleRetention int `json:"sampleRetention"`
	// Layer retention time, in months ('m'), days ('d'), or hours ('h').
	// Format: "[0-9]+[hmd]". For example: "3h", "7d", "1m"
	LayerRetentionTime string `json:"layerRetentionTime"`
}

type TableSchema struct {
	Version              int      `json:"version"`
	RollupLayers         []Rollup `json:"rollupLayers"`
	ShardingBucketsCount int      `json:"shardingBucketsCount"`
	PartitionerInterval  string   `json:"partitionerInterval"`
	ChunckerInterval     string   `json:"chunckerInterval"`
}

type PartitionSchema struct {
	Version                int      `json:"version"`
	Aggregates             []string `json:"aggregates"`
	AggregationGranularity string   `json:"aggregationGranularity"`
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
	Partitions          []*Partition    `json:"partitions"`
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

func GetOrDefaultConfig() (*V3ioConfig, error) {
	return GetOrLoadFromFile("")
}

func GetOrLoadFromFile(path string) (*V3ioConfig, error) {
	once.Do(func() {
		instance, failure = loadConfig(path)
		return
	})

	return instance, failure
}

func GetOrLoadFromData(data []byte) (*V3ioConfig, error) {
	once.Do(func() {
		instance, failure = loadFromData(data)
		return
	})

	return instance, failure
}

// Update the defaults when using a configuration structure
func GetOrLoadFromStruct(cfg *V3ioConfig) (*V3ioConfig, error) {
	once.Do(func() {
		initDefaults(cfg)
		instance = cfg
		return
	})

	return instance, nil
}

func loadConfig(path string) (*V3ioConfig, error) {

	var resolvedPath string

	if strings.TrimSpace(path) != "" {
		resolvedPath = path
	} else {
		envPath := os.Getenv(V3ioConfigEnvironmentVariable)
		if envPath != "" {
			resolvedPath = envPath
		}
	}

	if resolvedPath == "" {
		resolvedPath = DefaultConfigurationFileName
	}

	var data []byte
	if _, err := os.Stat(resolvedPath); err != nil {
		if os.IsNotExist(err) {
			data = []byte{}
		} else {
			return nil, errors.Wrap(err, "Failed to read the TSDB configuration.")
		}
	} else {
		data, err = ioutil.ReadFile(resolvedPath)
		if err != nil {
			return nil, err
		}

		if len(data) == 0 {
			return nil, errors.Errorf("Configuration file '%s' exists but its content is invalid.", resolvedPath)
		}
	}

	return loadFromData(data)
}

func loadFromData(data []byte) (*V3ioConfig, error) {
	cfg := V3ioConfig{}
	err := yaml.Unmarshal(data, &cfg)

	if err != nil {
		return nil, err
	}

	initDefaults(&cfg)

	return &cfg, err
}

func initDefaults(cfg *V3ioConfig) {
	// Initialize the default number of workers
	if cfg.Workers == 0 {
		cfg.Workers = defaultNumberOfIngestWorkers
	}

	// Initialize the default number of Query workers if not set to Min(8,Workers)
	if cfg.QryWorkers == 0 {
		if cfg.Workers < defaultNumberOfQueryWorkers {
			cfg.QryWorkers = cfg.Workers
		} else {
			cfg.QryWorkers = defaultNumberOfQueryWorkers
		}
	}

	// Initialize the default batch size
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}

	if cfg.DefaultTimeoutInSeconds == 0 {
		cfg.DefaultTimeoutInSeconds = int(defaultTimeoutInSeconds)
	}

	if cfg.MaximumChunkSize == 0 {
		cfg.MaximumChunkSize = defaultMaximumChunkSize
	}

	if cfg.MinimumChunkSize == 0 {
		cfg.MinimumChunkSize = defaultMinimumChunkSize
	}

	if cfg.MaximumSampleSize == 0 {
		cfg.MaximumSampleSize = defaultMaximumSampleSize
	}

	if cfg.MaximumPartitionSize == 0 {
		cfg.MaximumPartitionSize = defaultMaximumPartitionSize
	}

	if cfg.ShardingBucketsCount == 0 {
		cfg.ShardingBucketsCount = DefaultShardingBucketsCount
	}
}
