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
package tsdbtest

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
)

const TsdbDefaultTestConfigPath = "testdata"

// nolint: deadcode,varcheck
const relativeProjectPath = "src/github.com/v3io/v3io-tsdb"

/*
This method will try and load the configuration file from several locations by the following order:
1. Environment variable named 'V3IO_TSDB_CONFIG'
2. Current package's 'testdata/v3io-tsdb-config.yaml' folder
3. $GOPATH/src/github.com/v3io/v3io-tsdb/v3io-tsdb-config.yaml
*/
func GetV3ioConfigPath() (string, error) {
	if configurationPath := os.Getenv(config.V3ioConfigEnvironmentVariable); configurationPath != "" {
		return configurationPath, nil
	}

	localConfigFile := filepath.Join(TsdbDefaultTestConfigPath, config.DefaultConfigurationFileName)
	if _, err := os.Stat(localConfigFile); !os.IsNotExist(err) {
		return localConfigFile, nil
	}

	// Look for a parent directory containing a makefile and the configuration file (presumed to be the project root).
	dirPath := "./"
	for {
		_, err := os.Stat(dirPath + "Makefile")
		if err == nil {
			confFilePath := dirPath + config.DefaultConfigurationFileName
			_, err = os.Stat(confFilePath)
			if err == nil {
				return confFilePath, nil
			}
			break // Bail out if we found the makefile but the config is not there.
		}
		absolute, err := filepath.Abs(dirPath)
		if err != nil || absolute == "/" { // Bail out if we reached the root.
			break
		}
		dirPath += "../"
	}

	return "", errors.Errorf("config file is not specified and could not be found")
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
