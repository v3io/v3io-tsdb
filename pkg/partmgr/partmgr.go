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
	"github.com/v3io/v3io-go-http"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

// Create new Partition Manager, for now confined to one Cyclic partition
func NewPartitionMngr(cfg *config.Schema, path string, cont *v3io.Container) *PartitionManager {
	newMngr := &PartitionManager{cfg: cfg, path: path, cyclic: true, ignoreWrap: true, container: cont}
	for _, part := range cfg.Partitions {
		path := fmt.Sprintf("%s/%s/", newMngr.path, string(part.StartTime))
		newPart := NewDBPartition(newMngr, part.StartTime, path)
		newMngr.partitions = append(newMngr.partitions, newPart)
		if newMngr.headPartition == nil {
			newMngr.headPartition = newPart
		} else {
			if newMngr.headPartition.startTime < newPart.startTime {
				newMngr.headPartition = newPart
			}
		}
	}
	return newMngr
}

// Create and Init a new Partition
func NewDBPartition(pmgr *PartitionManager, startTime int64, path string) *DBPartition {
	newPart := DBPartition{
		manager:       pmgr,
		path:          path,
		startTime:     startTime,
		days:          1,//pmgr.cfg.DaysPerObj, //TODO
		hoursInChunk:  1,//pmgr.cfg.HrInChunk,
		prefix:        "",
		retentionDays: pmgr.cfg.PartitionSchemaInfo.SampleRetention,
		rollupTime:    int64(pmgr.cfg.PartitionSchemaInfo.AggregatorsGranularityInSeconds) * 60 * 1000,
	}

	aggrType, _ := aggregate.AggrsFromString(pmgr.cfg.PartitionSchemaInfo.Aggregators)
	newPart.defaultRollups = aggrType
	if pmgr.cfg.PartitionSchemaInfo.AggregatorsGranularityInSeconds != 0 {
		newPart.rollupBuckets = /*pmgr.cfg.DaysPerObj*/ 1 * 24 * 60 / pmgr.cfg.PartitionSchemaInfo.SampleRetention //TODO
	}

	return &newPart
}

type PartitionManager struct {
	mtx           sync.RWMutex
	path          string
	cfg           *config.Schema
	headPartition *DBPartition
	partitions    []*DBPartition
	cyclic        bool
	ignoreWrap    bool
	container     *v3io.Container
}

func (p *PartitionManager) IsCyclic() bool {
	return p.cyclic
}

func (p *PartitionManager) GetConfig() *config.Schema {
	return p.cfg
}

func (p *PartitionManager) Init() error {
	return nil
}

func (p *PartitionManager) TimeToPart(t int64) (*DBPartition, error) {
	if p.headPartition == nil {
		_, err := p.createNewPartition(p.TimeToPartitionStart(t))
		return p.headPartition, err
	} else {
		tNext := p.TimeToNextPartition(p.headPartition.startTime)
		if t >= p.headPartition.startTime && t < tNext {
			return p.headPartition, nil
		} else if t < p.headPartition.startTime {
			//iterate backwards, ignore last elem as it's the headPartition
			for i := len(p.partitions) - 2; i >= 0; i-- {
				if t > p.partitions[i].startTime {
					return p.partitions[i], nil
				}
			}
		} else {
			_, err := p.createNewPartition(tNext)
			if err != nil {
				return nil, err
			}
			return p.TimeToPart(t)
		}
	}
	return p.headPartition, nil
}

func (p *PartitionManager) createNewPartition(t int64) (*DBPartition, error) {
	time := t & 0x7FFFFFFFFFFFFFF0
	path := fmt.Sprintf("%s/%s/", p.path, string(time))
	partition := NewDBPartition(p, time, path)
	p.headPartition = partition
	p.partitions = append(p.partitions, partition)
	err := p.updatePartitionInSchema(partition)
	return partition, err
}

func (p *PartitionManager) updatePartitionInSchema(partition *DBPartition) error {
	p.cfg.Partitions = append(p.cfg.Partitions, config.Partition{StartTime:partition.startTime, SchemaInfo: p.cfg.PartitionSchemaInfo})
	data, err := json.Marshal(p.cfg)
	if err != nil {
		return errors.Wrap(err, "Failed to update new partition in schema file")
	}
	err = p.container.Sync.PutObject(&v3io.PutObjectInput{Path:p.path + tsdb.SCHEMA_CONFIG, Body: data})
	return err
}

func (p *PartitionManager) PartsForRange(mint, maxt int64) []*DBPartition {
	var parts []*DBPartition
	for _, part := range p.partitions {
		if part.startTime >= mint && part.startTime < maxt {
			parts = append(parts, part)
		}
	}
	return parts
}

func (p *PartitionManager) GetHead() *DBPartition {
	//TODO check nil
	return p.headPartition
}

func (p *PartitionManager) TimeToNextPartition(t int64) int64 {
	interval := int(p.cfg.PartitionSchemaInfo.PartitionerInterval[0])
	date := p.cfg.PartitionSchemaInfo.PartitionerInterval[1]
	switch date {
	//case 'Y':
	//case 'M':
	case 'D':
		d := t / 3600 / 24 / 1000
		return (d + int64(interval)) * 24 * 3600 * 1000
	case 'H':
		d,h := TimeToDHM(t)
		return int64((d * 24 + h + interval) * 3600 * 1000)
	//case 'm':
	}
	return 0
}

func (p *PartitionManager) TimeToPartitionStart(t int64) int64 {
	date := p.cfg.PartitionSchemaInfo.PartitionerInterval[1]
	if p.headPartition == nil {
		//first event
	} else {
		//p.headPartition.startTime   -> prev partition start
	}
	switch date {
	case 'Y':
	case 'M':
	case 'D':
		d := t / 3600 / 24 / 1000
		return d * 24 * 3600 * 1000
	case 'H':
		d,h := TimeToDHM(t)
		return int64((d * 24 + h) * 3600 * 1000)
	case 'm':
	//default:
	}
	return 0
}

type DBPartition struct {
	manager        *PartitionManager
	path           string             // Full path (in the DB) to the partition
	startTime      int64              // Start from time/date
	days           int                // Number of days stored in the partition
	hoursInChunk   int                // number of hours stored in each chunk
	prefix         string             // Path prefix
	retentionDays  int                // Keep samples for N hours
	defaultRollups aggregate.AggrType // Default Aggregation functions to apply on sample update
	rollupTime     int64              // Time range per aggregation bucket
	rollupBuckets  int                // Total number of buckets per partition
}

func (p *DBPartition) IsCyclic() bool {
	return p.manager.cyclic
}

// Time covered by a single chunk
func (p *DBPartition) TimePerChunk() int {
	return p.hoursInChunk * 3600 * 1000
}

func (p *DBPartition) NextPart(t int64) (*DBPartition, error) {
	return p.manager.TimeToPart(p.manager.TimeToNextPartition(t))
}

func (p *DBPartition) GetStartTime() int64 {
	return p.startTime
}

// return path to metrics table
func (p *DBPartition) GetTablePath() string {
	return p.path
}

// return list of Sharding Keys matching the name
func (p *DBPartition) GetShardingKeys(name string) []string {
	return []string{name}
}

// return metric object full path
func (p *DBPartition) GetMetricPath(name string, hash uint64) string {

	return fmt.Sprintf("%s%s.%016x", p.path, name, hash)
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
func (p *DBPartition) TimeToChunkId(tmilli int64) int {

	t := int(tmilli - p.startTime/1000)
	h := (t / 3600) % 24
	d := t / 3600 / 24

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

// return the mint and maxt for this partition, may need maxt for cyclic partition
// TODO: add non cyclic partitions
func (p *DBPartition) GetPartitionRange(maxt int64) (int64, int64) {
	// start p.days ago, rounded to next hour
	newMin := (maxt/1000/3600 - int64(p.days*24) + 1) * 3600 * 1000
	return newMin, maxt
}

// return the valid minimum time in a cyclic partition based on max time
func (p *DBPartition) CyclicMinTime(mint, maxt int64) int64 {
	// start p.days ago, rounded to next hour
	newMin := (maxt/1000/3600 - int64(p.days*24) + 1) * 3600 * 1000
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
