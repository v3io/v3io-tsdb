// +build integration

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

package tsdb_test

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"testing"
	"time"
)

type testTsdbSuite struct {
	suite.Suite
}

func (suite *testTsdbSuite) TestAppend() {

	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	suite.Require().Nil(err)

	defer tsdbtest.SetUp(suite.T(), v3ioConfig)()

	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, nil, nil)
	suite.Require().Nil(err)

	appender, err := adapter.Appender()
	suite.Require().Nil(err)

	querier, err := adapter.Querier(nil, 0, math.MaxInt64)
	suite.Require().Nil(err)

	_, err = appender.Add(
		utils.Labels{utils.Label{Name: "__name__", Value: "AAPL"}, utils.Label{Name: "market", Value: "usa"}},
		1541030400000,
		-91)
	suite.Require().Nil(err)
	_, err = appender.Add(
		utils.Labels{utils.Label{Name: "__name__", Value: "AAL"}, utils.Label{Name: "market", Value: "usa"}},
		1541030400000,
		-87)
	suite.Require().Nil(err)
	_, err = appender.Add(
		utils.Labels{utils.Label{Name: "__name__", Value: "AAP"}, utils.Label{Name: "market", Value: "usa"}},
		1541203200000,
		-50)
	suite.Require().Nil(err)

	_, err = appender.WaitForCompletion(0)
	suite.Require().Nil(err)

	set, err := querier.Select("", "min", int64(time.Hour/time.Millisecond), "1==1")
	suite.Require().Nil(err)

	// TODO: Replace map[tv]struct{} with []tv once TSDB-37 is fixed. This open issue causes duplicate results.
	var result = make(map[string]map[tv]struct{})
	for set.Next() {
		suite.Require().Nil(set.Err())
		key := set.At().Labels().String()
		var samples = make(map[tv]struct{})
		iter := set.At().Iterator()
		for iter.Next() {
			t, v := iter.At()
			samples[tv{t: t, v: v}] = struct{}{}
		}
		result[key] = samples
		fmt.Println(key, result[key])
	}

	expected := map[string]map[tv]struct{}{
		`{__name__="AAPL", market="usa", Aggregate="min"}`: {tv{t: 1541030400000, v: -91}: struct{}{}},
		`{__name__="AAL", market="usa", Aggregate="min"}`:  {tv{t: 1541030400000, v: -87}: struct{}{}},
		`{__name__="AAP", market="usa", Aggregate="min"}`:  {tv{t: 1541203200000, v: -50}: struct{}{}},
	}

	suite.Require().Equal(expected, result)
}

type tv struct {
	t int64
	v float64
}

func TestTsdbSuite(t *testing.T) {
	suite.Run(t, new(testTsdbSuite))
}
