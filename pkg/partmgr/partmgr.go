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
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"path"
	"strconv"
	"sync"
)

// Create a new partition manager
func NewPartitionMngr(schemaConfig *config.Schema, cont *v3io.Container, v3ioConfig *config.V3ioConfig) (*PartitionManager, error) {
	currentPartitionInterval, err := utils.Str2duration(schemaConfig.PartitionSchemaInfo.PartitionerInterval)
	if err != nil {
		return nil, err
	}
	newMngr := &PartitionManager{schemaConfig: schemaConfig, cyclic: false, container: cont, currentPartitionInterval: currentPartitionInterval, v3ioConfig: v3ioConfig}
	newMngr.updatePartitionsFromSchema(schemaConfig)
	return newMngr, nil
}

// Create and initialize a new partition
func NewDBPartition(pmgr *PartitionManager, startTime int64, path string) (*DBPartition, error) {
	rollupTime, err := utils.Str2duration(pmgr.schemaConfig.PartitionSchemaInfo.AggregationGranularity)
	if err != nil {
		return nil, err
	}
	partitionInterval, err := utils.Str2duration(pmgr.schemaConfig.PartitionSchemaInfo.PartitionerInterval)
	if err != nil {
		return nil, err
	}
	chunkInterval, err := utils.Str2duration(pmgr.schemaConfig.PartitionSchemaInfo.ChunckerInterval)
	if err != nil {
		return nil, err
	}
	newPart := DBPartition{
		manager:           pmgr,
		path:              path,
		startTime:         startTime,
		partitionInterval: partitionInterval,
		chunkInterval:     chunkInterval,
		prefix:            "",
		retentionDays:     pmgr.schemaConfig.PartitionSchemaInfo.SampleRetention,
		rollupTime:        rollupTime,
	}

	aggrType, err := aggregate.AggrsFromString(pmgr.schemaConfig.PartitionSchemaInfo.Aggregates)
	if err != nil {
		return nil, err
	}
	newPart.defaultRollups = aggrType
	if rollupTime != 0 {
		newPart.rollupBuckets = int(math.Ceil(float64(partitionInterval) / float64(rollupTime)))
	}

	return &newPart, nil
}

type PartitionManager struct {
	mtx                      sync.RWMutex
	schemaConfig             *config.Schema
	schemaMtimeSecs          int
	schemaMtimeNanosecs      int
	headPartition            *DBPartition
	partitions               []*DBPartition
	cyclic                   bool
	container                *v3io.Container
	currentPartitionInterval int64 //TODO update on schema changes
	v3ioConfig               *config.V3ioConfig
}

func (p *PartitionManager) Path() string {
	return p.v3ioConfig.TablePath
}

func (p *PartitionManager) GetPartitionsPaths() []string {
	var paths []string
	for _, part := range p.partitions {
		paths = append(paths, part.GetTablePath())
	}
	return paths
}

func (p *PartitionManager) GetConfig() *config.Schema {
	return p.schemaConfig
}

func (p *PartitionManager) Init() error {
	return nil
}

func (p *PartitionManager) TimeToPart(t int64) (*DBPartition, error) {
	if p.headPartition == nil {
		// Rounding t to the nearest PartitionInterval multiple
		_, err := p.createAndUpdatePartition(p.currentPartitionInterval * (t / p.currentPartitionInterval))
		return p.headPartition, err
	} else {
		if t >= p.headPartition.startTime {
			if (t - p.headPartition.startTime) >= p.currentPartitionInterval {
				_, err := p.createAndUpdatePartition(p.headPartition.startTime + p.currentPartitionInterval)
				if err != nil {
					return nil, err
				}
				return p.TimeToPart(t)
			} else {
				return p.headPartition, nil
			}
		} else {
			// Iterate backwards; ignore the last element as it's the head partition
			for i := len(p.partitions) - 2; i >= 0; i-- {
				if t > p.partitions[i].startTime {
					return p.partitions[i], nil
				}
			}
			head := p.headPartition
			part, _ := p.createAndUpdatePartition(p.currentPartitionInterval * (t / p.currentPartitionInterval))
			p.headPartition = head
			return part, nil
		}
	}
}

func (p *PartitionManager) createAndUpdatePartition(t int64) (*DBPartition, error) {
	time := t & 0x7FFFFFFFFFFFFFF0
	partPath := path.Join(p.Path(), strconv.FormatInt(time/1000, 10)) + "/"
	partition, err := NewDBPartition(p, time, partPath)
	if err != nil {
		return nil, err
	}
	p.currentPartitionInterval = partition.partitionInterval
	if p.headPartition == nil || time > p.headPartition.startTime {
		p.headPartition = partition
		p.partitions = append(p.partitions, partition)
	} else {
		for i, part := range p.partitions {
			if part.startTime > time {
				p.partitions = append(p.partitions, nil)
				copy(p.partitions[i+1:], p.partitions[i:])
				p.partitions[i] = partition
				break
			}
		}
	}
	p.schemaConfig.Partitions = append(p.schemaConfig.Partitions, &config.Partition{StartTime: partition.startTime, SchemaInfo: p.schemaConfig.PartitionSchemaInfo})
	err = p.updateSchema()
	return partition, err
}

func (p *PartitionManager) updateSchema() (err error) {

	metricReporter := performance.ReporterInstanceFromConfig(p.v3ioConfig)
	metricReporter.WithTimer("UpdateSchemaTimer", func() {
		data, err := json.Marshal(p.schemaConfig)
		if err != nil {
			err = errors.Wrap(err, "Failed to update a new partition in the schema file.")
			return
		}
		if p.container != nil { // Tests use case only
			err = p.container.Sync.PutObject(&v3io.PutObjectInput{Path: path.Join(p.Path(), config.SchemaConfigFileName), Body: data})
		}
	})

	return
}

func (p *PartitionManager) DeletePartitionsFromSchema(partitionsToDelete []*DBPartition) {
	for i := len(p.partitions) - 1; i >= 0; i-- {
		for _, partToDelete := range partitionsToDelete {
			if p.partitions[i].startTime == partToDelete.startTime {
				p.partitions = append(p.partitions[:i], p.partitions[i+1:]...)
				break
			}
		}

	}
	for i := len(p.schemaConfig.Partitions) - 1; i >= 0; i-- {
		for _, partToDelete := range partitionsToDelete {
			if p.schemaConfig.Partitions[i].StartTime == partToDelete.startTime {
				p.schemaConfig.Partitions = append(p.schemaConfig.Partitions[:i], p.schemaConfig.Partitions[i+1:]...)
				break
			}
		}

	}
	p.updateSchema()
}

func (p *PartitionManager) ReadAndUpdateSchema() (err error) {
	metricReporter, err := performance.DefaultReporterInstance()
	if err != nil {
		err = errors.Wrap(err, "Unable to initialize the performance-metrics reporter.")
		return
	}

	fullPath := path.Join(p.Path(), config.SchemaConfigFileName)
	if err != nil {
		err = errors.Wrap(err, "Failed to create timer ReadAndUpdateSchemaTimer.")
		return
	}
	schemaInfoResp, err := p.container.Sync.GetItem(&v3io.GetItemInput{Path: fullPath, AttributeNames: []string{"__mtime_secs", "__mtime_nsecs"}})
	if err != nil {
		err = errors.Wrapf(err, "Failed to read schema at path '%s'.", fullPath)
	}
	mtimeSecs, err := schemaInfoResp.Output.(*v3io.GetItemOutput).Item.GetFieldInt("__mtime_secs")
	if err != nil {
		err = errors.Wrapf(err, "Failed to get start time (mtime) in seconds from the schema at '%s'.", fullPath)
	}
	mtimeNsecs, err := schemaInfoResp.Output.(*v3io.GetItemOutput).Item.GetFieldInt("__mtime_nsecs")
	if err != nil {
		err = errors.Wrapf(err, "Failed to get start time (mtime) in nanoseconds from the schema at '%s'.", fullPath)
	}

	// Get schema only if the schema has changed
	if mtimeSecs > p.schemaMtimeSecs || (mtimeSecs == p.schemaMtimeSecs && mtimeNsecs > p.schemaMtimeNanosecs) {
		p.schemaMtimeSecs = mtimeSecs
		p.schemaMtimeNanosecs = mtimeNsecs

		metricReporter.WithTimer("ReadAndUpdateSchemaTimer", func() {
			resp, err := p.container.Sync.GetObject(&v3io.GetObjectInput{Path: fullPath})
			if err != nil {
				err = errors.Wrapf(err, "Failed to read schema at path '%s'.", fullPath)
			}

			schema := &config.Schema{}
			err = json.Unmarshal(resp.Body(), schema)
			if err != nil {
				err = errors.Wrapf(err, "Failed to unmarshal schema at path '%s'.", fullPath)
			}
			p.schemaConfig = schema
			p.updatePartitionsFromSchema(schema)

		})
	}
	return
}

func (p *PartitionManager) updatePartitionsFromSchema(schema *config.Schema) error {
	p.partitions = []*DBPartition{}
	for _, part := range schema.Partitions {
		partPath := path.Join(p.Path(), strconv.FormatInt(part.StartTime/1000, 10)) + "/"
		newPart, err := NewDBPartition(p, part.StartTime, partPath)
		if err != nil {
			return err
		}
		p.partitions = append(p.partitions, newPart)
		if p.headPartition == nil {
			p.headPartition = newPart
		} else if p.headPartition.startTime < newPart.startTime {
			p.headPartition = newPart
		}
	}
	return nil
}

func (p *PartitionManager) PartsForRange(mint, maxt int64) []*DBPartition {
	var parts []*DBPartition
	for _, part := range p.partitions {
		if part.InRange(mint) || part.InRange(maxt) || (mint < part.GetStartTime() && maxt > part.GetEndTime()) {
			parts = append(parts, part)
		}
	}
	return parts
}

type DBPartition struct {
	manager           *PartitionManager
	path              string             // Full path to the partition within the DB
	startTime         int64              // Start time
	partitionInterval int64              // Number of msecs stored in the partition
	chunkInterval     int64              // Number of msecs stored in each chunk
	prefix            string             // Path prefix
	retentionDays     int                // Keep samples for N hours
	defaultRollups    aggregate.AggrType // Default aggregation functions to apply on sample update
	rollupTime        int64              // Time range per aggregation bucket
	rollupBuckets     int                // Total number of aggregation buckets per partition
}

func (p *DBPartition) IsCyclic() bool {
	return p.manager.cyclic
}

// Return the time range covered by a single chunk (the chunk interval)
func (p *DBPartition) TimePerChunk() int64 {
	return p.chunkInterval
}

func (p *DBPartition) NextPart(t int64) (*DBPartition, error) {
	return p.manager.TimeToPart(t)
}

func (p *DBPartition) GetStartTime() int64 {
	return p.startTime
}

func (p *DBPartition) GetEndTime() int64 {
	return p.startTime + p.partitionInterval - 1
}

// Return the path to this partition's TSDB table
func (p *DBPartition) GetTablePath() string {
	return p.path
}

// Return a list of sharding keys matching the given item name
func (p *DBPartition) GetShardingKeys(name string) []string {
	shardingKeysNum := p.manager.schemaConfig.TableSchemaInfo.ShardingBucketsCount
	var res = make([]string, 0, shardingKeysNum)
	for i := 0; i < shardingKeysNum; i++ {
		// Trailing period ('.') for range-scan queries
		res = append(res, fmt.Sprintf("%s_%x.", name, i))
	}

	return res
}

// Return the full path to the specified metric item
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

// Return the aggregation bucket ID for the specified time
func (p *DBPartition) Time2Bucket(t int64) int {
	if p.rollupTime == 0 {
		return 0
	}
	if t > p.GetEndTime() {
		return p.rollupBuckets - 1
	}
	if t < p.GetStartTime() {
		return 0
	}
	return int((t - p.startTime) / p.rollupTime)
}

// Return the nearest chunk start time for the specified time
func (p *DBPartition) GetChunkMint(t int64) int64 {
	if t > p.GetEndTime() {
		return p.GetEndTime() - p.chunkInterval + 1
	}
	if t < p.GetStartTime() {
		return p.startTime
	}
	return p.chunkInterval * (t / p.chunkInterval)
}

// Check whether the specified time (t) is within the range of the chunk starting at the specified start time (mint)
func (p *DBPartition) InChunkRange(mint, t int64) bool {
	return t >= mint && t < (mint+p.chunkInterval)
}

// Check whether the specified time (t) is ahead of the range of the chunk starting at the specified start time (mint)
func (p *DBPartition) IsAheadOfChunk(mint, t int64) bool {
	return t >= (mint + p.chunkInterval)
}

// Return the ID of the chunk whose range includes the specified time
func (p *DBPartition) TimeToChunkId(tmilli int64) (int, error) {
	if tmilli >= p.startTime && tmilli <= p.GetEndTime() {
		return int((tmilli-p.startTime)/p.chunkInterval) + 1, nil
	} else {
		return -1, errors.Errorf("Time %d isn't within the range of this partition.", tmilli)
	}
}

// Check whether the specified time is within the range of this partition
func (p *DBPartition) InRange(t int64) bool {
	if p.manager.cyclic {
		return true
	}
	return t >= p.startTime && t <= p.GetEndTime()
}

// Return the start time (mint) and end time (maxt) for this partition;
// maxt may be required for a cyclic partition
func (p *DBPartition) GetPartitionRange() (int64, int64) {
	// Start p.days ago, rounded to next hour
	return p.startTime, p.startTime + p.partitionInterval
}

// Return the attribute name of the given chunk
func (p *DBPartition) ChunkID2Attr(col string, id int) string {
	return fmt.Sprintf("_%s%d", col, id)
}

// Return the attributes that need to be retrieved for the specified time range
func (p *DBPartition) Range2Attrs(col string, mint, maxt int64) ([]string, int64) {
	list := p.Range2Cids(mint, maxt)
	var strList []string
	for _, id := range list {
		strList = append(strList, p.ChunkID2Attr(col, id))
	}

	firstAttrTime := p.startTime + ((mint-p.startTime)/p.chunkInterval)*p.chunkInterval
	return strList, firstAttrTime
}

// Return a list of all the chunk IDs that match the specified time range
func (p *DBPartition) Range2Cids(mint, maxt int64) []int {
	var list []int
	start, err := p.TimeToChunkId(mint)
	if err != nil {
		start = 1
	}
	end, err := p.TimeToChunkId(maxt)
	if err != nil {
		end = int(p.partitionInterval / p.chunkInterval)
	}
	for i := start; i <= end; i++ {
		list = append(list, i)
	}
	return list
}

func (p *DBPartition) GetHashingBuckets() int {
	return p.manager.schemaConfig.TableSchemaInfo.ShardingBucketsCount
}

// Convert a time in milliseconds to day and hour integers
func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	h := (t / 3600) % 24
	d := t / 3600 / 24
	return d, h
}
