// +build unit

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
