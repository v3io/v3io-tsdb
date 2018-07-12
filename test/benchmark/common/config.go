package common

import (
	"github.com/ghodss/yaml"
	"io/ioutil"
	"log"
	"os"
	"fmt"
	"github.com/pkg/errors"
)

const TsdbBenchIngestConfig = "TSDB_BENCH_INGEST_CONFIG"
const TsdbV3ioConfig = "V3IO_TSDBCFG_PATH"

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
}

func LoadBenchmarkIngestConfigFromData() (*BenchmarkIngestConfig, error) {
	var benchConfigFile = os.Getenv(TsdbBenchIngestConfig)
	if benchConfigFile == "" {
		benchConfigFile = "tsdb-bench-test-config.yaml"
	}

	configData, err := ioutil.ReadFile(benchConfigFile)
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}

	cfg := BenchmarkIngestConfig{}
	err = yaml.Unmarshal(configData, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Failed to load config from file %s", benchConfigFile))
	}

	initDefaults(&cfg)

	return &cfg, err
}

func initDefaults(cfg *BenchmarkIngestConfig) {
	if cfg.StartTimeOffset == "" {
		cfg.StartTimeOffset = "48h"
	}
}
