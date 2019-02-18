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

package frames

import (
	"encoding/json"
	"fmt"
	"os"
)

// LogConfig is the logging configuration
type LogConfig struct {
	Level string `json:"level,omitempty"`
}

// Config is server configuration
type Config struct {
	Log            LogConfig `json:"log"`
	DefaultLimit   int       `json:"limit,omitempty"`
	DefaultTimeout int       `json:"timeout,omitempty"`

	// default V3IO connection details
	WebAPIEndpoint string `json:"webApiEndpoint"`
	Container      string `json:"container"`
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`
	SessionKey     string `json:"sessionKey,omitempty"`

	// Number of parallel V3IO worker routines
	Workers int `json:"workers"`

	Backends []*BackendConfig `json:"backends,omitempty"`
}

// InitDefaults initializes the defaults for configuration
func (c *Config) InitDefaults() error {
	if c.DefaultTimeout == 0 {
		c.DefaultTimeout = 30
	}

	return nil
}

// InitSessionDefaults initializes session defaults
func InitSessionDefaults(session *Session, framesConfig *Config) *Session {

	if session == nil {
		session = &Session{}
	}

	if session.Url == "" {
		session.Url = framesConfig.WebAPIEndpoint
	}
	if session.Container == "" {
		session.Container = framesConfig.Container
	}
	if session.User == "" {
		session.User = framesConfig.Username
	}
	if session.Password == "" {
		session.Password = framesConfig.Password
	}
	if session.Token == "" {
		session.Token = framesConfig.SessionKey
	}

	return session
}

// InitBackendDefaults initializes default configuration for backend
func InitBackendDefaults(cfg *BackendConfig, framesConfig *Config) {
	if cfg.Workers == 0 {
		cfg.Workers = framesConfig.Workers
		if cfg.Workers == 0 {
			cfg.Workers = 8
		}
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends")
	}

	names := make(map[string]bool)

	for i, backend := range c.Backends {
		if backend.Name == "" {
			backend.Name = backend.Type
		}

		if backend.Type == "" {
			return fmt.Errorf("backend %q missing type", backend.Name)
		}

		if found := names[backend.Name]; found {
			return fmt.Errorf("backend %d - duplicate name %q", i, backend.Name)
		}

		names[backend.Name] = true
	}

	return nil
}

// BackendConfig is default backend configuration
type BackendConfig struct {
	Type    string `json:"type"` // v3io, csv, ...
	Name    string `json:"name"`
	Workers int    `json:"workers"`
	// backend specific options
	Options map[string]interface{} `json:"options"`

	// CSV backend
	RootDir string `json:"rootdir,omitempty"`
}

// NewSession will create a new session. It will populate missing values from
// the V3IO_SESSION environment variable (JSON encoded)
func NewSession(url, container, path, user, password, token, id string) (*Session, error) {
	env, err := envSession()
	if err != nil {
		return nil, err
	}

	// TODO: Use reflect (see sessionFields pb/method.go)
	session := &Session{
		Url:       firstVal(url, env.Url),
		Container: firstVal(container, env.Container),
		Path:      firstVal(path, env.Path),
		User:      firstVal(user, env.User),
		Password:  firstVal(password, env.Password),
		Token:     firstVal(token, env.Token),
		Id:        firstVal(id, env.Id),
	}

	return session, nil
}

func envSession() (*Session, error) {
	var envSession Session
	data := os.Getenv("V3IO_SESSION")
	if len(data) == 0 {
		return &envSession, nil
	}

	if err := json.Unmarshal([]byte(data), &envSession); err != nil {
		return nil, err
	}

	return &envSession, nil
}

func firstVal(args ...string) string {
	for _, arg := range args {
		if len(arg) > 0 {
			return arg
		}
	}

	return ""
}
