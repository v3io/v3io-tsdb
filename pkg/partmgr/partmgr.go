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
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"strconv"
	"sync"
)

// Create new Partition Manager
func NewPartitionMngr(cfg *config.Schema, path string, cont *v3io.Container) *PartitionManager {
	newMngr := &PartitionManager{cfg: cfg, path: path, cyclic: false, ignoreWrap: true, container: cont}
	for _, part := range cfg.Partitions {
		path := fmt.Sprintf("%s/%d/", newMngr.path, part.StartTime)
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
		manager:           pmgr,
		path:              path,
		startTime:         startTime,
		partitionInterval: IntervalInMilli(pmgr.cfg.PartitionSchemaInfo.PartitionerInterval),
		chunkInterval:     IntervalInMilli(pmgr.cfg.PartitionSchemaInfo.ChunckerInterval),
		prefix:            "",
		retentionDays:     pmgr.cfg.PartitionSchemaInfo.SampleRetention,
		rollupTime:        int64(pmgr.cfg.PartitionSchemaInfo.AggregatorsGranularityInSeconds) * 60 * 1000,
	}

	aggrType, _ := aggregate.AggrsFromString(pmgr.cfg.PartitionSchemaInfo.Aggregators)
	newPart.defaultRollups = aggrType
	if pmgr.cfg.PartitionSchemaInfo.AggregatorsGranularityInSeconds != 0 {
		newPart.rollupBuckets = /*pmgr.cfg.DaysPerObj*/ 1 * 24 * 60 / pmgr.cfg.PartitionSchemaInfo.AggregatorsGranularityInSeconds //TODO
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
	interval := IntervalInMilli(p.cfg.PartitionSchemaInfo.PartitionerInterval)
	if p.headPartition == nil {
		_, err := p.createNewPartition(interval * (t / interval))
		return p.headPartition, err
	} else {
		if t >= p.headPartition.startTime {
			if (t - p.headPartition.startTime) > interval {
				_, err := p.createNewPartition(p.headPartition.startTime + interval)
				if err != nil {
					return nil, err
				}
				return p.TimeToPart(t)
			} else {
				return p.headPartition, nil
			}
		} else {
			//iterate backwards, ignore last elem as it's the headPartition
			for i := len(p.partitions) - 2; i >= 0; i-- {
				if t > p.partitions[i].startTime {
					return p.partitions[i], nil
				}
			}
		}
	}
	return p.headPartition, nil
}

func (p *PartitionManager) createNewPartition(t int64) (*DBPartition, error) {
	time := t & 0x7FFFFFFFFFFFFFF0
	path := fmt.Sprintf("%s/%d/", p.path, time)
	partition := NewDBPartition(p, time, path)
	p.headPartition = partition
	p.partitions = append(p.partitions, partition)
	err := p.updatePartitionInSchema(partition)
	return partition, err
}

func (p *PartitionManager) updatePartitionInSchema(partition *DBPartition) error {
	p.cfg.Partitions = append(p.cfg.Partitions, config.Partition{StartTime: partition.startTime, SchemaInfo: p.cfg.PartitionSchemaInfo})
	data, err := json.Marshal(p.cfg)
	if err != nil {
		return errors.Wrap(err, "Failed to update new partition in schema file")
	}
	err = p.container.Sync.PutObject(&v3io.PutObjectInput{Path: p.path + config.SCHEMA_CONFIG, Body: data})
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
	return p.headPartition
}

type DBPartition struct {
	manager           *PartitionManager
	path              string             // Full path (in the DB) to the partition
	startTime         int64              // Start from time/date
	partitionInterval int64              // Number of millis stored in the partition
	chunkInterval     int64              // number of millis stored in each chunk
	prefix            string             // Path prefix
	retentionDays     int                // Keep samples for N hours
	defaultRollups    aggregate.AggrType // Default Aggregation functions to apply on sample update
	rollupTime        int64              // Time range per aggregation bucket
	rollupBuckets     int                // Total number of buckets per partition
}

func (p *DBPartition) IsCyclic() bool {
	return p.manager.cyclic
}

// Time covered by a single chunk
func (p *DBPartition) TimePerChunk() int64 {
	return p.chunkInterval
}

func (p *DBPartition) NextPart(t int64) (*DBPartition, error) {
	return p.manager.TimeToPart(t)
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
	var res []string
	for i := 0; i < p.manager.cfg.TableSchemaInfo.ShardingBuckets; i++ {
		res = append(res, fmt.Sprintf("%s_%x", name, i))
	}
	return res
}

// return metric object full path
func (p *DBPartition) GetMetricPath(name string, hash uint64) string {

	return fmt.Sprintf("%s%s_%x.%016x", p.path, name, int(hash%uint64(p.GetHashingBuckets())), hash)
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
	interval := IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.ChunckerInterval)
	return interval * (t / interval)
}

// is the time t in the range of the chunk starting at mint
func (p *DBPartition) InChunkRange(mint, t int64) bool {
	return t >= mint && t < (mint+IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.ChunckerInterval))
}

// is the time t ahead of the range of the chunk starting at mint
func (p *DBPartition) IsAheadOfChunk(mint, t int64) bool {
	return t >= (mint + IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.ChunckerInterval))
}

// Get ID of the Chunk covering time t
func (p *DBPartition) TimeToChunkId(tmilli int64) int {
	return int((tmilli-p.startTime)/IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.ChunckerInterval)) + 1
}

// is t covered by this partition
func (p *DBPartition) InRange(t int64) bool {
	if p.manager.cyclic {
		return true
	}
	return (t >= p.startTime) && (t < p.startTime+p.partitionInterval)
}

// return the mint and maxt for this partition, may need maxt for cyclic partition
// TODO: add non cyclic partitions
func (p *DBPartition) GetPartitionRange(maxt int64) (int64, int64) {
	// start p.days ago, rounded to next hour
	maxtInHours := maxt/1000/3600 + 1
	intervalInHours := IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.PartitionerInterval) / (3600 * 1000)
	newMin := (maxtInHours - intervalInHours) * 3600 * 1000
	return newMin, maxt
}

// return the valid minimum time in a cyclic partition based on max time
func (p *DBPartition) CyclicMinTime(mint, maxt int64) int64 {
	// start p.days ago, rounded to next hour
	newMin, _ := p.GetPartitionRange(maxt)
	if mint > newMin {
		return mint
	}
	return newMin
}

// Attribute name of a chunk
func (p *DBPartition) ChunkID2Attr(col string, id int) string {
	return fmt.Sprintf("_%s%d", col, id)
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
	chunkInterval := IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.ChunckerInterval)
	partitionInterval := IntervalInMilli(p.manager.cfg.PartitionSchemaInfo.PartitionerInterval)
	chunks := partitionInterval / chunkInterval

	if end < start {
		for i := start; int64(i) < chunks; i++ {
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

func (p *DBPartition) GetHashingBuckets() int {
	return p.manager.cfg.TableSchemaInfo.ShardingBuckets
}

// Convert time in milisec to Day index and hour
func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	//m := t/60 - ((t/3600) * 60)
	h := (t / 3600) % 24
	d := t / 3600 / 24
	return d, h
}

func IntervalInMilli(format string) int64 {
	interval, _ := strconv.Atoi(format[0 : len(format)-1])
	unit := format[len(format)-1]
	minutesVal := int64(interval * 60 * 1000)
	switch unit {
	case 'Y':
		return 365 * 24 * 60 * minutesVal
	case 'M':
		return 30 * 24 * 60 * minutesVal
	case 'D':
		return 24 * 60 * minutesVal
	case 'H':
		return 60 * minutesVal
	case 'm':
		return minutesVal
	}
	return 0
}
