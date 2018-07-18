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
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"sync"
)

// Create new Partition Manager, for now confined to one Cyclic partition
func NewPartitionMngr(cfg *config.DBPartConfig, path string) *PartitionManager {
	newMngr := &PartitionManager{cfg: cfg, path: path, cyclic: true, ignoreWrap: true}
	newMngr.headPartition = NewDBPartition(newMngr)
	return newMngr
}

// Create and Init a new Partition
func NewDBPartition(pmgr *PartitionManager) *DBPartition {
	newPart := DBPartition{
		manager:       pmgr,
		path:          pmgr.path + "/0/", // TODO: format a string based on id & format
		partID:        1,
		startTime:     0,
		days:          pmgr.cfg.DaysPerObj,
		hoursInChunk:  pmgr.cfg.HrInChunk,
		prefix:        "",
		retentionDays: pmgr.cfg.DaysRetention,
		rollupTime:    int64(pmgr.cfg.RollupMin) * 60 * 1000,
	}

	aggrType, _ := aggregate.AggrsFromString(pmgr.cfg.DefaultRollups) // TODO: error check & load part data from schema object
	newPart.defaultRollups = aggrType
	if pmgr.cfg.RollupMin != 0 {
		newPart.rollupBuckets = pmgr.cfg.DaysPerObj * 24 * 60 / pmgr.cfg.RollupMin
	}

	return &newPart
}

type PartitionManager struct {
	mtx           sync.RWMutex
	path          string
	cfg           *config.DBPartConfig
	headPartition *DBPartition
	cyclic        bool
	ignoreWrap    bool
}

func (p *PartitionManager) IsCyclic() bool {
	return p.cyclic
}

func (p *PartitionManager) GetConfig() *config.DBPartConfig {
	return p.cfg
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

func (p *PartitionManager) GetHead() *DBPartition {

	return p.headPartition
}

type DBPartition struct {
	manager        *PartitionManager
	path           string             // Full path (in the DB) to the partition
	partID         int                // PartitionID
	startTime      int64              // Start from time/date
	days           int                // Number of days stored in the partition
	hoursInChunk   int                // number of hours stored in each chunk
	prefix         string             // Path prefix
	retentionDays  int                // Keep samples for N days
	defaultRollups aggregate.AggrType // Default Aggregation functions to apply on sample update
	rollupTime     int64              // Time range per aggregation bucket
	rollupBuckets  int                // Total number of buckets per partition
}

func (p *DBPartition) IsCyclic() bool {
	return p.manager.cyclic
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

func (p *DBPartition) GetPath() string {
	return p.path
}

func (p *DBPartition) AggrType() aggregate.AggrType {
	return p.defaultRollups
}

func (p *DBPartition) AggrBuckets() int {
	return p.rollupBuckets
}

func (p *DBPartition) RollupTime() int64 {
	return p.rollupTime
}

// get aggregator bucket id
func (p *DBPartition) Time2Bucket(t int64) int {
	if p.rollupTime == 0 {
		return 0
	}
	return int((t-p.startTime)/p.rollupTime) % p.rollupBuckets
}

// get nearest chunk start
func (p *DBPartition) GetChunkMint(t int64) int64 {
	return (t / 3600 / 1000 / int64(p.hoursInChunk)) * 3600 * 1000 * int64(p.hoursInChunk)
}

// is the time t in the range of the chunk starting at mint
func (p *DBPartition) InChunkRange(mint, t int64) bool {
	return t >= mint && t < (mint+3600*1000*int64(p.hoursInChunk))
}

// is the time t ahead of the range of the chunk starting at mint
func (p *DBPartition) IsAheadOfChunk(mint, t int64) bool {
	return t >= (mint + 3600*1000*int64(p.hoursInChunk))
}

// Get ID of the Chunk covering time t
func (p *DBPartition) TimeToChunkId(t int64) int {
	d, h := TimeToDHM(t - p.startTime)

	if p.days <= 1 {
		return h
	}

	dayIndex := d % p.days
	chunkIdx := dayIndex*24/p.hoursInChunk + h/p.hoursInChunk
	return chunkIdx
}

// is t covered by this partition
func (p *DBPartition) InRange(t int64) bool {
	if p.manager.cyclic {
		return true
	}
	return (t >= p.startTime) && (t < p.startTime+int64(p.days)*24*3600*1000)
}

// return the valid minimum time in a cyclic partition based on max time
func (p *DBPartition) CyclicMinTime(mint, maxt int64) int64 {
	maxSec := maxt / 1000
	//if !p.manager.ignoreWrap {
	//	maxSec = time.Now().Unix()
	//}
	// start p.days ago, rounded to next hour
	newMin := (maxSec/3600 - int64(p.days*24) + 1) * 3600 * 1000
	if mint > newMin {
		return mint
	}
	return newMin
}

// Attribute name of a chunk
func (p *DBPartition) ChunkID2Attr(col string, id int) string {
	return fmt.Sprintf("_%s%d", col, id*p.hoursInChunk)
}

// Return the attributes that need to be retrieved for a given time range
func (p *DBPartition) Range2Attrs(col string, mint, maxt int64) ([]string, []int) {
	list := p.Range2Cids(mint, maxt)
	strList := []string{}
	for _, id := range list {
		strList = append(strList, p.ChunkID2Attr(col, id))
	}
	return strList, list
}

// All the chunk IDs which match the time range
func (p *DBPartition) Range2Cids(mint, maxt int64) []int {
	list := []int{}
	start := p.TimeToChunkId(mint)
	end := p.TimeToChunkId(maxt)
	chunks := p.days * 24 / p.hoursInChunk

	if end < start {
		for i := start; i < chunks; i++ {
			list = append(list, i)
		}
		for i := 0; i <= end; i++ {
			list = append(list, i)
		}

		return list
	}

	for i := start; i <= end; i++ {
		list = append(list, i)
	}
	return list
}

// Convert time in milisec to Day index and hour
func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	//m := t/60 - ((t/3600) * 60)
	h := (t / 3600) % 24
	d := t / 3600 / 24
	return d, h
}
