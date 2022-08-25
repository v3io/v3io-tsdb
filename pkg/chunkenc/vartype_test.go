// +build unit

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

package chunkenc

import (
	"fmt"
	"testing"
	"time"

	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
)

type testVarEncoderSuite struct {
	suite.Suite
}

func (suite *testVarEncoderSuite) TestStringEnc() {

	logger, err := nucliozap.NewNuclioZapTest("test")
	suite.Require().Nil(err)

	chunk := newVarChunk(logger)
	appender, err := chunk.Appender()
	suite.Require().Nil(err)

	list := []string{"abc", "", "123456"}
	t0 := time.Now().UnixNano() / 1000

	for i, s := range list {
		t := t0 + int64(i*1000)
		appender.Append(t, s)
	}

	iterChunk, err := FromData(logger, EncVariant, chunk.Bytes(), 0)
	suite.Require().Nil(err)

	iter := iterChunk.Iterator()
	i := 0
	for iter.Next() {
		t, v := iter.AtString()
		suite.Require().Equal(t, t0+int64(i*1000))
		suite.Require().Equal(v, list[i])
		fmt.Println("t, v: ", t, v)
		i++
	}

	suite.Require().Nil(iter.Err())
	suite.Require().Equal(i, len(list))
}

func TestVarEncoderSuite(t *testing.T) {
	suite.Run(t, new(testVarEncoderSuite))
}
