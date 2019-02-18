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

// ServerState is state of server
type ServerState string

// Possible server states
const (
	ReadyState   ServerState = "ready"
	RunningState ServerState = "running"
	ErrorState   ServerState = "error"
)

// Server is frames server interface
type Server interface {
	Start() error
	State() ServerState
	Err() error
}

// ServerBase have common functionality for server
type ServerBase struct {
	err   error
	state ServerState
}

// NewServerBase returns a new server base
func NewServerBase() *ServerBase {
	return &ServerBase{
		state: ReadyState,
	}
}

// Err returns the server error
func (s *ServerBase) Err() error {
	return s.err
}

// SetState sets the server state
func (s *ServerBase) SetState(state ServerState) {
	s.state = state
}

// State return the server state
func (s *ServerBase) State() ServerState {
	return s.state
}

// SetError sets current error and will change state to ErrorState
func (s *ServerBase) SetError(err error) {
	s.err = err
	s.state = ErrorState
}
