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

package v3ioutil

import (
	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
)

const KEY_SEPERATOR = "="

func NewLogger(verbose bool) (logger.Logger, error) {
	var logLevel nucliozap.Level
	if verbose {
		logLevel = nucliozap.DebugLevel
	} else {
		logLevel = nucliozap.WarnLevel
	}
	log, err := nucliozap.NewNuclioZapCmd("v3io-prom", logLevel)
	if err != nil {
		return nil, err
	}
	return log, nil
}

func CreateContainer(logger logger.Logger, addr, cont string, workers int) (*v3io.Container, error) {
	// create context
	context, err := v3io.NewContext(logger, addr, workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}

	// create session
	session, err := context.NewSession("", "", "v3test")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create session")
	}

	// create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create container")
	}

	return container, nil
}
