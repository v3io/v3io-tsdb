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

package partmgr

import (
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
)

func TestCreateNewPartition(tst *testing.T) {
	schema := testutils.CreateSchema(tst, "*")
	interval, _ := utils.Str2duration(schema.PartitionSchemaInfo.PartitionerInterval)
	manager, _ := NewPartitionMngr(&schema, "/", nil)
	startTime := interval + 1
	//first partition
	part, _ := manager.TimeToPart(startTime + interval)
	assert.Equal(tst, 1, len(manager.partitions))
	assert.Equal(tst, manager.headPartition, part)
	//new head
	part, _ = manager.TimeToPart(startTime + (interval * 3))
	assert.Equal(tst, 3, len(manager.partitions))
	assert.Equal(tst, manager.headPartition, part)
	//add in the middle
	part, _ = manager.TimeToPart(startTime + (interval * 2))
	assert.Equal(tst, 3, len(manager.partitions))
	assert.Equal(tst, manager.partitions[1], part)
	//add first
	part, _ = manager.TimeToPart(startTime)
	assert.Equal(tst, 4, len(manager.partitions))
	assert.Equal(tst, manager.partitions[0], part)
}
