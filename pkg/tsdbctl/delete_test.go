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
package tsdbctl

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type testDeleteSuite struct {
	suite.Suite
}

func (suite *testDeleteSuite) TestNewDeleteCommandeerErrorOnAllAndBeginConflict() {
	commandeer := newDeleteCommandeer(&RootCommandeer{})
	commandeer.deleteAll = true
	commandeer.fromTime = "123"
	err := commandeer.cmd.RunE(commandeer.cmd, nil)
	suite.Error(err)
	suite.Equal("delete --all cannot be used in conjunction with --begin", err.Error())
}

func (suite *testDeleteSuite) TestNewDeleteCommandeerErrorOnAllAndFilterConflict() {
	commandeer := newDeleteCommandeer(&RootCommandeer{})
	commandeer.deleteAll = true
	commandeer.filter = "some filter"
	err := commandeer.cmd.RunE(commandeer.cmd, nil)
	suite.Error(err)
	suite.Equal("delete --all cannot be used in conjunction with --filter", err.Error())
}

func TestDeleteSuite(t *testing.T) {
	suite.Run(t, new(testDeleteSuite))
}
