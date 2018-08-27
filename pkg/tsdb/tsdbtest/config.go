package tsdbtest

import (
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"os"
	"path/filepath"
	"strings"
)

const TsdbDefaultTestConfigPath = "testdata"
const relativeProjectPath = "src/github.com/v3io/v3io-tsdb"

/*
This method will try and load the configuration file from several locations by the following order:
1. Environment variable named 'V3IO_CONF'
2. Current package's 'testdata/v3io.yaml' folder
3. $GOPATH/src/github.com/v3io/v3io-tsdb/v3io.yaml
*/
func GetV3ioConfigPath() (string, error) {
	if configurationPath := os.Getenv(config.V3ioConfigEnvironmentVariable); configurationPath != "" {
		return configurationPath, nil
	}

	localConfigFile := filepath.Join(TsdbDefaultTestConfigPath, config.DefaultConfigurationFileName)
	if _, err := os.Stat(localConfigFile); !os.IsNotExist(err) {
		return localConfigFile, nil
	}

	gopath := strings.Split(os.Getenv("GOPATH"), string(os.PathListSeparator))
	for _, path := range gopath {
		gopathConfig := filepath.Join(path, relativeProjectPath, config.DefaultConfigurationFileName)
		if _, err := os.Stat(gopathConfig); !os.IsNotExist(err) {
			return gopathConfig, nil
		}
	}

	return "", errors.New("config file is not specified")
}

func LoadV3ioConfig() (*config.V3ioConfig, error) {
	path, err := GetV3ioConfigPath()
	if err != nil {
		return nil, err
	}
	v3ioConfig, err := config.LoadConfig(path)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to load test configuration.")
	}
	return v3ioConfig, nil
}
