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

package backends

import (
	"fmt"
	"strings"
	"sync"

	"github.com/nuclio/logger"
	"github.com/v3io/frames"
)

var (
	factories     map[string]Factory
	lock          sync.RWMutex
	normalizeType = strings.ToLower
)

// Factory is a backend factory
type Factory func(logger.Logger, *frames.BackendConfig, *frames.Config) (frames.DataBackend, error)

// Register registers a backend factory for a type
func Register(typ string, factory Factory) error {
	lock.Lock()
	defer lock.Unlock()

	if factories == nil {
		factories = make(map[string]Factory)
	}

	typ = normalizeType(typ)
	if _, ok := factories[typ]; ok {
		return fmt.Errorf("backend %q already registered", typ)
	}

	factories[typ] = factory
	return nil
}

// GetFactory returns factory for a backend
func GetFactory(typ string) Factory {
	lock.RLock()
	defer lock.RUnlock()

	return factories[normalizeType(typ)]
}
