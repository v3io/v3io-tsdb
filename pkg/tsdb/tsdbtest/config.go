package tsdbtest

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"go/build"
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

	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		gopath = build.Default.GOPATH
	}
	gopaths := strings.Split(gopath, string(os.PathListSeparator))
	for _, path := range gopaths {
		gopathConfig := filepath.Join(path, relativeProjectPath, config.DefaultConfigurationFileName)
		if _, err := os.Stat(gopathConfig); !os.IsNotExist(err) {
			return gopathConfig, nil
		}
	}

	return "", errors.Errorf("config file is not specified and could not be found in GOPATH=%v", gopath)
}

func LoadV3ioConfig() (*config.V3ioConfig, error) {
	path, err := GetV3ioConfigPath()
	if err != nil {
		return nil, err
	}
	v3ioConfig, err := config.GetOrLoadFromFile(path)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unable to load test configuration from '%s'", path))
	}
	return v3ioConfig, nil
}
