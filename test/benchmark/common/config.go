package common

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"io/ioutil"
	"os"
)

const TsdbBenchIngestConfig = "TSDB_BENCH_INGEST_CONFIG"

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
	FlushFrequency       int    `json:"FlushFrequency,omitempty" yaml:"FlushFrequency"`
	AppendOneByOne       bool   `json:"AppendOneByOne,omitempty" yaml:"AppendOneByOne"`
}

func LoadBenchmarkIngestConfigs() (*BenchmarkIngestConfig, *config.V3ioConfig, error) {
	testConfig, err := loadBenchmarkIngestConfigFromFile("")
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to load test configuration.")
	}
	v3ioCinfig, err := config.LoadConfig("")
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to load test configuration.")
	}

	return testConfig, v3ioCinfig, nil
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
		benchConfigFile = "tsdb-bench-test-config.yaml"
	}

	configData, err := ioutil.ReadFile(benchConfigFile)
	if err != nil {
		return nil, errors.Errorf("Failed to load config from file %s", benchConfigFile)
	}

	return loadBenchmarkIngestConfigFromData(configData)
}

func initDefaults(cfg *BenchmarkIngestConfig) {
	if cfg.StartTimeOffset == "" {
		cfg.StartTimeOffset = "48h"
	}
}
