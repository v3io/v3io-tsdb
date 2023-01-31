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
package common

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
)

const TsdbBenchIngestConfig = "TSDB_BENCH_INGEST_CONFIG"
const TsdbDefaultTestConfigPath = "testdata"

type BenchmarkIngestConfig struct {
	Verbose              bool   `json:"Verbose,omitempty" yaml:"Verbose"`
	StartTimeOffset      string `json:"StartTimeOffset,omitempty" yaml:"StartTimeOffset"`
	SampleStepSize       int    `json:"SampleStepSize,omitempty" yaml:"SampleStepSize"`
	NamesCount           int    `json:"NamesCount,omitempty" yaml:"NamesCount"`
	NamesDiversity       int    `json:"NamesDiversity,omitempty" yaml:"NamesDiversity"`
	LabelsCount          int    `json:"LabelsCount,omitempty" yaml:"LabelsCount"`
	LabelsDiversity      int    `json:"LabelsDiversity,omitempty" yaml:"LabelsDiversity"`
	LabelValuesCount     int    `json:"LabelValuesCount,omitempty" yaml:"LabelValuesCount"`
	LabelsValueDiversity int    `json:"LabelsValueDiversity,omitempty" yaml:"LabelsValueDiversity"`
	AppendOneByOne       bool   `json:"AppendOneByOne,omitempty" yaml:"AppendOneByOne"`
	BatchSize            int    `json:"BatchSize,omitempty" yaml:"BatchSize"`
	CleanupAfterTest     bool   `json:"CleanupAfterTest,omitempty" yaml:"CleanupAfterTest"`
	QueryAggregateStep   string `json:"QueryAggregateStep,omitempty" yaml:"QueryAggregateStep"`
	ValidateRawData      bool   `json:"ValidateRawData,omitempty" yaml:"ValidateRawData"`
}

func LoadBenchmarkIngestConfigs() (*BenchmarkIngestConfig, *config.V3ioConfig, error) {
	testConfig, err := loadBenchmarkIngestConfigFromFile("")
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to load test configuration.")
	}
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to load test configuration.")
	}

	if testConfig.BatchSize == 0 && v3ioConfig.BatchSize > 0 {
		// Use batch size from V3IO config
		testConfig.BatchSize = v3ioConfig.BatchSize
	}

	return testConfig, v3ioConfig, nil
}

func loadBenchmarkIngestConfigFromData(configData []byte) (*BenchmarkIngestConfig, error) {
	cfg := BenchmarkIngestConfig{}
	err := yaml.Unmarshal(configData, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Unable to parse configuration: %s", string(configData)))
	}

	initDefaults(&cfg)

	return &cfg, err
}

func loadBenchmarkIngestConfigFromFile(benchConfigFile string) (*BenchmarkIngestConfig, error) {
	if benchConfigFile == "" {
		benchConfigFile = os.Getenv(TsdbBenchIngestConfig)
	}

	if benchConfigFile == "" {
		benchConfigFile = filepath.Join(TsdbDefaultTestConfigPath, "tsdb-bench-test-config.yaml") // relative path
	}

	configData, err := os.ReadFile(benchConfigFile)
	if err != nil {
		return nil, errors.Errorf("failed to load config from file %s", benchConfigFile)
	}

	return loadBenchmarkIngestConfigFromData(configData)
}

func initDefaults(cfg *BenchmarkIngestConfig) {
	if cfg.StartTimeOffset == "" {
		cfg.StartTimeOffset = "48h"
	}

	if cfg.QueryAggregateStep == "" {
		cfg.QueryAggregateStep = "5m"
	}
}
