// +build unit

package tsdbtest

import (
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"go/build"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createTestConfig(t *testing.T, path string) {
	fullPath := filepath.Join(path, config.DefaultConfigurationFileName)
	_, err := os.Create(fullPath)
	if err != nil {
		t.Fatalf("Failed to create file at %s. Error: %v", fullPath, err)
	}
	t.Logf("---> Created test configuration at: %s", fullPath)
}

func deleteTestConfig(t *testing.T, path string) {
	fullPath := filepath.Join(path, config.DefaultConfigurationFileName)
	err := os.Remove(fullPath)
	if err != nil && !os.IsNotExist(err) {
		t.Errorf("Failed to remove file at %s. Error: %v", fullPath, err)
	}
	t.Logf("<--- Removed test configuration from: %s", fullPath)
}

func TestGetV3ioConfigPath(t *testing.T) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		gopath = build.Default.GOPATH
	}
	firstGoPath := strings.Split(gopath, string(os.PathListSeparator))[0]
	projectHome := filepath.Join(firstGoPath, relativeProjectPath)
	testCases := []struct {
		description  string
		expectedPath string
		setup        func() func()
	}{
		{description: "get config from package testdata",
			expectedPath: filepath.Join(TsdbDefaultTestConfigPath, config.DefaultConfigurationFileName),
			setup: func() func() {
				// Make this test agnostic to environment variables at runtime (store & recover on exit)
				configPathEnv := os.Getenv(config.V3ioConfigEnvironmentVariable)
				os.Unsetenv(config.V3ioConfigEnvironmentVariable)

				if _, err := os.Stat(filepath.Join(TsdbDefaultTestConfigPath, config.DefaultConfigurationFileName)); !os.IsNotExist(err) {
					return func() {
						os.Setenv(config.V3ioConfigEnvironmentVariable, configPathEnv)
					}
				} else {
					path := TsdbDefaultTestConfigPath
					if err := os.Mkdir(path, 0777); err != nil && !os.IsExist(err) {
						t.Fatalf("Failed to mkdir %v", err)
					}
					createTestConfig(t, path)
					return func() {
						os.Setenv(config.V3ioConfigEnvironmentVariable, configPathEnv)
						deleteTestConfig(t, path)
						os.RemoveAll(path)
					}
				}
			}},

		{description: "get config from project root",
			expectedPath: filepath.Join(projectHome, config.DefaultConfigurationFileName),
			setup: func() func() {
				// Make this test agnostic to environment variables at runtime (store & recover on exit)
				configPathEnv := os.Getenv(config.V3ioConfigEnvironmentVariable)
				os.Unsetenv(config.V3ioConfigEnvironmentVariable)

				if _, err := os.Stat(filepath.Join(projectHome, config.DefaultConfigurationFileName)); !os.IsNotExist(err) {
					return func() {
						os.Setenv(config.V3ioConfigEnvironmentVariable, configPathEnv)
					}
				} else {
					path := projectHome
					createTestConfig(t, path)
					return func() {
						os.Setenv(config.V3ioConfigEnvironmentVariable, configPathEnv)
						deleteTestConfig(t, path)
						os.Remove(path)
					}
				}
			}},

		{description: "get config from env var",
			expectedPath: getConfigPathFromEnvOrDefault(),
			setup: func() func() {
				env := os.Getenv(config.V3ioConfigEnvironmentVariable)
				if env == "" {
					os.Setenv(config.V3ioConfigEnvironmentVariable, config.DefaultConfigurationFileName)
					return func() {
						os.Unsetenv(config.V3ioConfigEnvironmentVariable)
					}
				}
				return func() {}
			}},
	}

	for _, test := range testCases {
		t.Run(test.description, func(t *testing.T) {
			testGetV3ioConfigPathCase(t, test.expectedPath, test.setup)
		})
	}
}

func getConfigPathFromEnvOrDefault() string {
	configPath := os.Getenv(config.V3ioConfigEnvironmentVariable)
	if configPath == "" {
		configPath = config.DefaultConfigurationFileName
	}
	return configPath
}

func testGetV3ioConfigPathCase(t *testing.T, expected string, setup func() func()) {
	defer setup()()
	path, err := GetV3ioConfigPath()
	if err != nil {
		t.Fatal("Failed to get configuration path", err)
	}
	assert.Equal(t, expected, path)
}

func TestMergeConfig(t *testing.T) {
	defaultCfg, err := config.GetOrDefaultConfig()
	if err != nil {
		t.Fatal("Failed to get default configuration", err)
	}

	updateWithCfg := config.V3ioConfig{
		BatchSize: 128,
		TablePath: "test-new-table",
		MetricsReporter: config.MetricsReporterConfig{
			ReportOnShutdown: true,
			RepotInterval:    120,
		},
	}

	mergedCfg, err := defaultCfg.Merge(&updateWithCfg)
	if err != nil {
		t.Fatal("Failed to update default configuration", err)
	}

	// Validate result structure
	assert.Equal(t, mergedCfg.BatchSize, 128)
	assert.Equal(t, mergedCfg.TablePath, "test-new-table")
	assert.Equal(t, mergedCfg.MetricsReporter.ReportOnShutdown, true)
	assert.Equal(t, mergedCfg.MetricsReporter.RepotInterval, 120)

	// Make sure that default configuration remains unchanged
	snapshot, err := config.GetOrDefaultConfig()
	if err != nil {
		t.Fatal("Failed to get default configuration", err)
	}

	assert.Equal(t, snapshot.BatchSize, defaultCfg.BatchSize)
	assert.Equal(t, snapshot.TablePath, defaultCfg.TablePath)
	assert.Equal(t, snapshot.MetricsReporter.ReportOnShutdown, defaultCfg.MetricsReporter.ReportOnShutdown)
	assert.Equal(t, snapshot.MetricsReporter.RepotInterval, defaultCfg.MetricsReporter.RepotInterval)
}

func TestWithDefaults(t *testing.T) {
	myCfg := &config.V3ioConfig{
		BatchSize: 1024,
		TablePath: "test-my-table",
		MetricsReporter: config.MetricsReporterConfig{
			ReportOnShutdown:   true,
			RepotInterval:      180,
			ReportPeriodically: true,
		},
	}

	updatedCfg := config.WithDefaults(myCfg)

	// Make sure it didn't override anything
	assert.Equal(t, updatedCfg.BatchSize, myCfg.BatchSize)
	assert.Equal(t, updatedCfg.TablePath, myCfg.TablePath)
	assert.Equal(t, updatedCfg.MetricsReporter.ReportPeriodically, myCfg.MetricsReporter.ReportPeriodically)
	assert.Equal(t, updatedCfg.MetricsReporter.RepotInterval, myCfg.MetricsReporter.RepotInterval)
	assert.Equal(t, updatedCfg.MetricsReporter.ReportOnShutdown, myCfg.MetricsReporter.ReportOnShutdown)

	// and default value is set for ShardingBucketsCount
	assert.Equal(t, updatedCfg.ShardingBucketsCount, config.DefaultShardingBucketsCount)

	// WithDefaults method does not create new configuration struct, therefore result object has the same address as myCfg
	assert.Equal(t, myCfg, updatedCfg)
}
