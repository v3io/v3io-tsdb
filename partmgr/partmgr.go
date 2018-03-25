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

// TODO: need to expand from a single partition to multiple

package partmgr

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/config"
	"sync"
	"time"
)

func NewPartitionMngr(cfg *config.TsdbConfig) *PartitionManager {
	newMngr := &PartitionManager{cyclic: true}
	newMngr.headPartition = NewDBPartition(newMngr)
	return newMngr
}

func NewDBPartition(pmgr *PartitionManager) *DBPartition {
	newPart := DBPartition{
		manager:       pmgr,
		partID:        1,
		startTime:     0,
		days:          1,
		hoursInChunk:  1,
		prefix:        "",
		retentionDays: 0,
	}
	return &newPart
}

type PartitionManager struct {
	mtx           sync.RWMutex
	headPartition *DBPartition
	cyclic        bool
}

func (p *PartitionManager) Init() error {

	return nil
}

func (p *PartitionManager) TimeToPart(t int64) *DBPartition {

	return p.headPartition // TODO: find the matching partition, if newer create one
}

func (p *PartitionManager) PartsForRange(mint, maxt int64) []*DBPartition {

	return []*DBPartition{p.headPartition}
}

type DBPartition struct {
	manager       *PartitionManager
	partID        int
	startTime     int64
	days          int
	hoursInChunk  int
	prefix        string
	retentionDays int
}

func (p *DBPartition) HoursInChunk() int {
	return p.hoursInChunk
}

func (p *DBPartition) NextPart(t int64) *DBPartition {
	return p.manager.TimeToPart(t)
}

func (p *DBPartition) GetId() int {
	return p.partID
}

// get nearest chunk start
func (p *DBPartition) GetChunkMint(t int64) int64 {
	return (t / 3600 / 1000 / int64(p.hoursInChunk)) * 3600 * 1000 * int64(p.hoursInChunk)
}

func (p *DBPartition) InChunkRange(mint, t int64) bool {
	return t >= mint && t < (mint+3600*1000*int64(p.hoursInChunk))
}

func (p *DBPartition) IsAheadOfChunk(mint, t int64) bool {
	return t >= (mint + 3600*1000*int64(p.hoursInChunk))
}

func (p *DBPartition) TimeToChunkId(t int64) int {
	d, h := TimeToDHM(t - p.startTime)

	if p.days <= 1 {
		return h
	}

	dayIndex := d - ((d / p.days) * p.days)
	chunkIdx := dayIndex*24/p.hoursInChunk + h/p.hoursInChunk
	return chunkIdx
}

func (p *DBPartition) TimeRange() (int64, int64) {
	from := p.startTime
	if p.manager.cyclic {
		from = (time.Now().Unix()/3600 - int64(p.days*24) + 1) * 24 * 3600 * 1000 // start p.days ago, hour rounded
	}
	return from, from + int64(p.days*24*3600*1000)
}

func (p *DBPartition) ChunkID2Attr(col string, id int) string {
	return fmt.Sprintf("__%s%d", col, id*p.hoursInChunk)
}

func (p *DBPartition) Range2Attrs(col string, mint, maxt int64) ([]string, []int) {
	list := p.Range2Cids(mint, maxt)
	strList := []string{}
	for _, id := range list {
		strList = append(strList, p.ChunkID2Attr(col, id))
	}
	return strList, list
}

func (p *DBPartition) Range2Cids(mint, maxt int64) []int {
	list := []int{}
	start := p.TimeToChunkId(mint)
	end := p.TimeToChunkId(maxt)

	for i := start; i < 24 && i <= end; i++ {
		list = append(list, i)
	}
	if end < start {
		for i := 0; i <= end; i++ {
			list = append(list, i)
		}

	}

	return list
}

func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	//m := t/60 - ((t/3600) * 60)
	h := t/3600 - ((t / 3600 / 24) * 24)
	d := t / 3600 / 24
	return d, h
}
