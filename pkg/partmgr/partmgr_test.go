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
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"testing"
)

func TestCreateNewPartition(tst *testing.T) {
	manager := getPartitionManager(tst)
	interval := manager.currentPartitionInterval
	startTime := interval + 1

	// First partition
	part, err := manager.TimeToPart(startTime + interval)
	assert.Nil(tst, err, "Failed converting time to a partition.")
	if err != nil {
		tst.FailNow()
	}
	assert.Equal(tst, 1, len(manager.partitions))
	assert.Equal(tst, manager.headPartition, part)

	// New head
	part, err = manager.TimeToPart(startTime + (interval * 3))
	assert.Nil(tst, err, "Failed converting time to a partition.")
	if err != nil {
		tst.FailNow()
	}
	assert.Equal(tst, 3, len(manager.partitions))
	assert.Equal(tst, manager.headPartition, part)

	// Add (insert) in the middle
	part, err = manager.TimeToPart(startTime + (interval * 2))
	assert.Nil(tst, err, "Failed converting time to a partition.")
	if err != nil {
		tst.FailNow()
	}
	assert.Equal(tst, 3, len(manager.partitions))
	assert.Equal(tst, manager.partitions[1], part)

	// Add first
	part, err = manager.TimeToPart(startTime)
	assert.Nil(tst, err, "Failed converting time to a partition.")
	if err != nil {
		tst.FailNow()
	}
	assert.Equal(tst, 4, len(manager.partitions))
	assert.Equal(tst, manager.partitions[0], part)
}

func getPartitionManager(tst *testing.T) *PartitionManager {
	const dummyConfig = `path: "/test"`
	v3ioConfig, err := config.GetOrLoadFromData([]byte(dummyConfig))
	if err != nil {
		tst.Fatalf("Failed to obtain a TSDB configuration. Error: %v", err)
	}

	schm, err := schema.NewSchema(v3ioConfig, "1/s", "1h", "*")
	if err != nil {
		tst.Fatalf("Failed to create a TSDB schema. Error: %v", err)
	}

	manager, err := NewPartitionMngr(schm, nil, v3ioConfig)
	if err != nil {
		tst.Fatalf("Failed to create a partition manager. Error: %v", err)
	}

	return manager
}

func TestPartsForRange(tst *testing.T) {
	numPartitions := 5
	manager := getPartitionManager(tst)
	interval := manager.currentPartitionInterval
	for i := 1; i <= numPartitions; i++ {
		_, err := manager.TimeToPart(interval * int64(i))
		assert.Nil(tst, err, "Failed converting time to a partition.")
		if err != nil {
			tst.FailNow()
		}
	}
	assert.Equal(tst, numPartitions, len(manager.partitions))
	// Get all partitions
	assert.Equal(tst, manager.partitions, manager.PartsForRange(0, interval*int64(numPartitions+1), true))
	// Get no partitions
	assert.Equal(tst, 0, len(manager.PartsForRange(0, interval-1, true)), true)
	// Get the first 2 partitions
	parts := manager.PartsForRange(0, interval*2+1, true)
	assert.Equal(tst, 2, len(parts))
	assert.Equal(tst, manager.partitions[0], parts[0])
	assert.Equal(tst, manager.partitions[1], parts[1])
	// Get the middle 3 partitions
	parts = manager.PartsForRange(interval*2, interval*4+1, true)
	assert.Equal(tst, 3, len(parts))
	assert.Equal(tst, manager.partitions[1], parts[0])
	assert.Equal(tst, manager.partitions[2], parts[1])
	assert.Equal(tst, manager.partitions[3], parts[2])
	// Get the middle partition by inclusive=false
	parts = manager.PartsForRange(interval*2+1, interval*4+1, false)
	assert.Equal(tst, 1, len(parts))
	assert.Equal(tst, manager.partitions[2], parts[0])
	// Get the middle partition by inclusive=false
	parts = manager.PartsForRange(interval*2-1, interval*4+1, false)
	assert.Equal(tst, 2, len(parts))
	assert.Equal(tst, manager.partitions[1], parts[0])
	assert.Equal(tst, manager.partitions[2], parts[1])
}

func TestTime2Bucket(tst *testing.T) {
	manager := getPartitionManager(tst)
	part, _ := manager.TimeToPart(1000000)
	assert.Equal(tst, 0, part.Time2Bucket(100))
	assert.Equal(tst, part.rollupBuckets-1, part.Time2Bucket(part.startTime+part.partitionInterval+1))
	assert.Equal(tst, part.rollupBuckets/2, part.Time2Bucket((part.startTime+part.partitionInterval)/2))
}

func TestGetChunkMint(tst *testing.T) {
	manager := getPartitionManager(tst)
	part, err := manager.TimeToPart(manager.currentPartitionInterval)
	assert.Nil(tst, err, "Failed converting time to a partition.")
	if err != nil {
		tst.FailNow()
	}
	assert.Equal(tst, part.startTime, part.GetChunkMint(0))
	assert.Equal(tst, part.startTime, part.GetChunkMint(part.startTime+1))
	assert.Equal(tst, part.startTime+part.chunkInterval, part.GetChunkMint(part.startTime+part.chunkInterval+100))
	assert.Equal(tst, part.GetEndTime()-part.chunkInterval+1, part.GetChunkMint(part.GetEndTime()+100))
}

func TestInRange(tst *testing.T) {
	manager := getPartitionManager(tst)
	part, _ := manager.TimeToPart(manager.currentPartitionInterval)
	assert.Equal(tst, false, part.InRange(part.GetStartTime()-100))
	assert.Equal(tst, false, part.InRange(part.GetEndTime()+100))
	assert.Equal(tst, true, part.InRange(part.GetStartTime()+part.partitionInterval/2))
}

func TestRange2Cids(tst *testing.T) {
	manager := getPartitionManager(tst)
	part, _ := manager.TimeToPart(manager.currentPartitionInterval)
	numChunks := int(part.partitionInterval / part.chunkInterval)
	var cids []int
	for i := 1; i <= numChunks; i++ {
		cids = append(cids, i)
	}
	assert.Equal(tst, cids, part.Range2Cids(0, part.GetEndTime()+100))
	assert.Equal(tst, []int{3, 4, 5}, part.Range2Cids(part.startTime+2*part.chunkInterval, part.startTime+5*part.chunkInterval-1))
}
