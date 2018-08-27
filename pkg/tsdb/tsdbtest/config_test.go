// +build unit

package tsdbtest

import (
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createTestConfig(t *testing.T, path string) {
	_, err := os.Create(filepath.Join(path, config.DefaultConfigurationFileName))
	if err != nil {
		t.Fatalf("Failed to create file at %s", path)
	}
}

func TestGetV3ioConfigPath(t *testing.T) {
	firstGoPath := strings.Split(os.Getenv("GOPATH"), string(os.PathListSeparator))[0]
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
					if err := os.Mkdir(path, 0777); err != nil {
						t.Fatalf("Failed to mkdir %v", err)
					}
					createTestConfig(t, path)
					return func() {
						os.Setenv(config.V3ioConfigEnvironmentVariable, configPathEnv)
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
