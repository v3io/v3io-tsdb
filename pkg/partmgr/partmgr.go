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
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// Create a new partition manager
func NewPartitionMngr(schemaConfig *config.Schema, cont v3io.Container, v3ioConfig *config.V3ioConfig) (*PartitionManager, error) {
	currentPartitionInterval, err := utils.Str2duration(schemaConfig.PartitionSchemaInfo.PartitionerInterval)
	if err != nil {
		return nil, err
	}
	newMngr := &PartitionManager{schemaConfig: schemaConfig, cyclic: false, container: cont, currentPartitionInterval: currentPartitionInterval, v3ioConfig: v3ioConfig}
	err = newMngr.updatePartitionsFromSchema(schemaConfig)
	if err != nil {
		return nil, err
	}
	return newMngr, nil
}

type PartitionManager struct {
	mtx                      sync.RWMutex
	schemaConfig             *config.Schema
	schemaMtimeSecs          int
	schemaMtimeNanosecs      int
	headPartition            *DBPartition
	partitions               []*DBPartition
	cyclic                   bool
	container                v3io.Container
	currentPartitionInterval int64 //TODO update on schema changes
	v3ioConfig               *config.V3ioConfig
}

func (p *PartitionManager) GetPartitionsTablePath() string {
	return path.Join(p.Path(), "partitions")
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
				_, err := p.createAndUpdatePartition(p.currentPartitionInterval * (t / p.currentPartitionInterval))
				if err != nil {
					return nil, err
				}
			}
			return p.headPartition, nil
		} else {
			// Iterate backwards; ignore the last element as it's the head partition
			for i := len(p.partitions) - 2; i >= 0; i-- {
				if t >= p.partitions[i].startTime {
					if t < p.partitions[i].GetEndTime() {
						return p.partitions[i], nil
					} else {
						part, err := p.createAndUpdatePartition(p.currentPartitionInterval * (t / p.currentPartitionInterval))
						if err != nil {
							return nil, err
						}
						return part, nil
					}
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

	schemaPartition := &config.Partition{StartTime: partition.startTime, SchemaInfo: p.schemaConfig.PartitionSchemaInfo}
	if p.headPartition == nil || time > p.headPartition.startTime {
		p.headPartition = partition
		p.partitions = append(p.partitions, partition)
		p.schemaConfig.Partitions = append(p.schemaConfig.Partitions, schemaPartition)
	} else {
		for i, part := range p.partitions {
			if part.startTime > time {
				p.partitions = append(p.partitions, nil)
				copy(p.partitions[i+1:], p.partitions[i:])
				p.partitions[i] = partition

				p.schemaConfig.Partitions = append(p.schemaConfig.Partitions, nil)
				copy(p.schemaConfig.Partitions[i+1:], p.schemaConfig.Partitions[i:])
				p.schemaConfig.Partitions[i] = schemaPartition
				break
			}
		}
	}

	err = p.updateSchema()
	return partition, err
}

func (p *PartitionManager) updateSchema() error {

	var outerError error
	metricReporter := performance.ReporterInstanceFromConfig(p.v3ioConfig)
	metricReporter.WithTimer("UpdateSchemaTimer", func() {
		// updating schema version and copying partitions to kv table.
		p.schemaConfig.TableSchemaInfo.Version = schema.Version

		data, err := json.Marshal(p.schemaConfig)
		if err != nil {
			outerError = errors.Wrap(err, "Failed to update a new partition in the schema file.")
			return
		}
		if p.container != nil { // Tests use case only
			err = p.container.PutObjectSync(&v3io.PutObjectInput{Path: path.Join(p.Path(), config.SchemaConfigFileName), Body: data})
			if err != nil {
				outerError = err
				return
			}
			items := make(map[string]map[string]interface{}, len(p.partitions))
			for _, part := range p.partitions {
				items[strconv.FormatInt(part.startTime, 10)] = part.ToMap()
			}

			input := &v3io.PutItemsInput{Path: p.GetPartitionsTablePath(), Items: items}
			resp, err := p.container.PutItemsSync(input)

			if err != nil {
				outerError = errors.Wrap(err, "failed to update partitions table.")
				return
			}
			output := resp.Output.(*v3io.PutItemsOutput)
			if !output.Success {
				outerError = fmt.Errorf("got one or more errors, err: %v", output.Errors)
				return
			}
		}
	})

	return outerError
}

func (p *PartitionManager) DeletePartitionsFromSchema(partitionsToDelete []*DBPartition) error {
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

	// Delete from partitions KV table
	if p.container != nil { // Tests use case only
		for _, partToDelete := range partitionsToDelete {
			err := p.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path.Join(p.GetPartitionsTablePath(), strconv.FormatInt(partToDelete.startTime, 10))})
			if err != nil {
				return err
			}
		}
	}

	return p.updateSchema()
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
	schemaInfoResp, err := p.container.GetItemSync(&v3io.GetItemInput{Path: fullPath, AttributeNames: []string{"__mtime_secs", "__mtime_nsecs"}})
	if err != nil {
		err = errors.Wrapf(err, "Failed to read schema at path '%s'.", fullPath)
		return
	}
	mtimeSecs, err := schemaInfoResp.Output.(*v3io.GetItemOutput).Item.GetFieldInt("__mtime_secs")
	if err != nil {
		err = errors.Wrapf(err, "Failed to get start time (mtime) in seconds from the schema at '%s'.", fullPath)
		return
	}
	mtimeNsecs, err := schemaInfoResp.Output.(*v3io.GetItemOutput).Item.GetFieldInt("__mtime_nsecs")
	if err != nil {
		err = errors.Wrapf(err, "Failed to get start time (mtime) in nanoseconds from the schema at '%s'.", fullPath)
		return
	}

	// Get schema only if the schema has changed
	if mtimeSecs > p.schemaMtimeSecs || (mtimeSecs == p.schemaMtimeSecs && mtimeNsecs > p.schemaMtimeNanosecs) {
		p.schemaMtimeSecs = mtimeSecs
		p.schemaMtimeNanosecs = mtimeNsecs

		metricReporter.WithTimer("ReadAndUpdateSchemaTimer", func() {
			resp, innerError := p.container.GetObjectSync(&v3io.GetObjectInput{Path: fullPath})
			if innerError != nil {
				err = errors.Wrapf(innerError, "Failed to read schema at path '%s'.", fullPath)
				return
			}

			schema := &config.Schema{}
			innerError = json.Unmarshal(resp.Body(), schema)
			if innerError != nil {
				err = errors.Wrapf(innerError, "Failed to unmarshal schema at path '%s'.", fullPath)
				return
			}
			p.schemaConfig = schema
			innerError = p.updatePartitionsFromSchema(schema)
			if innerError != nil {
				err = errors.Wrapf(innerError, "Failed to update partitions from schema at path '%s'.", fullPath)
				return
			}
		})
	}
	return
}

func (p *PartitionManager) updatePartitionsFromSchema(schema *config.Schema) error {
	if schema.TableSchemaInfo.Version == 3 && !p.v3ioConfig.LoadPartitionsFromSchemaFile {
		return p.newLoadPartitions()
	}

	return p.oldLoadPartitions(schema)
}

func (p *PartitionManager) oldLoadPartitions(schema *config.Schema) error {
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

func (p *PartitionManager) newLoadPartitions() error {
	if p.container == nil { // Tests use case only
		return nil
	}

	getItems := &v3io.GetItemsInput{Path: p.GetPartitionsTablePath() + "/",
		AttributeNames: []string{"*"}}

	logger, err := utils.NewLogger(p.v3ioConfig.LogLevel)
	if err != nil {
		return err
	}
	iter, err := utils.NewAsyncItemsCursor(p.container, getItems, p.v3ioConfig.QryWorkers, []string{}, logger)
	if err != nil {
		return err
	}

	p.partitions = []*DBPartition{}
	for iter.Next() {
		startTime := iter.GetField(config.ObjectNameAttrName).(string)
		intStartTime, err := strconv.ParseInt(startTime, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid partition name '%v'", startTime)
		}

		partPath := path.Join(p.Path(), strconv.FormatInt(intStartTime/1000, 10)) + "/"
		newPart, err := NewDBPartitionFromMap(p, intStartTime, partPath, iter.GetItem())
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

	sort.SliceStable(p.partitions, func(i, j int) bool {
		return p.partitions[i].startTime < p.partitions[j].startTime
	})

	return nil
}

//if inclusive is true than partial partitions (not fully in range) will be retireved as well
func (p *PartitionManager) PartsForRange(mint, maxt int64, inclusive bool) []*DBPartition {
	var parts []*DBPartition
	for _, part := range p.partitions {
		if (mint < part.GetStartTime() && maxt > part.GetEndTime()) || (inclusive && (part.InRange(mint) || part.InRange(maxt))) {
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

	aggrType, _, err := aggregate.AggregatesFromStringListWithCount(pmgr.schemaConfig.PartitionSchemaInfo.Aggregates)
	if err != nil {
		return nil, err
	}
	newPart.defaultRollups = aggrType
	if rollupTime != 0 {
		newPart.rollupBuckets = int(math.Ceil(float64(partitionInterval) / float64(rollupTime)))
	}

	return &newPart, nil
}

// Create and initialize a new partition
func NewDBPartitionFromMap(pmgr *PartitionManager, startTime int64, path string, item v3io.Item) (*DBPartition, error) {
	rollupTime, err := item.GetFieldInt("rollupTime")
	if err != nil {
		return nil, fmt.Errorf("failed to parse rollupTime for partition: %v, rollup: %v", startTime, item.GetField("rollupTime"))
	}

	partitionInterval, err := item.GetFieldInt("partitionInterval")
	if err != nil {
		return nil, fmt.Errorf("failed to parse partitionInterval for partition: %v, interval: %v", startTime, item.GetField("partitionInterval"))
	}

	chunkInterval, err := item.GetFieldInt("chunkInterval")
	if err != nil {
		return nil, fmt.Errorf("failed to parse chunk Interval for partition: %v, interval: %v", startTime, item.GetField("chunkInterval"))
	}

	retention, err := item.GetFieldInt("retentionDays")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse retention days for partition: %v, retention: %v", startTime, item.GetField("retentionDays"))
	}

	stringAggregates, err := item.GetFieldString("aggregates")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse aggregates for partition: %v, aggregates: %v", startTime, item.GetField("aggregates"))
	}
	mask, _, err := aggregate.AggregatesFromStringListWithCount(strings.Split(stringAggregates, ","))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse aggregates from string for partition: %v, aggregates: %v", startTime, stringAggregates)
	}

	newPart := DBPartition{
		manager:           pmgr,
		path:              path,
		startTime:         startTime,
		partitionInterval: int64(partitionInterval),
		chunkInterval:     int64(chunkInterval),
		prefix:            "",
		retentionDays:     retention,
		rollupTime:        int64(rollupTime),
		defaultRollups:    mask,
	}

	if rollupTime != 0 {
		newPart.rollupBuckets = int(math.Ceil(float64(partitionInterval) / float64(rollupTime)))
	}

	return &newPart, nil
}

func (p *DBPartition) PreAggregates() []config.PreAggregate {
	return p.manager.GetConfig().TableSchemaInfo.PreAggregates
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
func (p *DBPartition) GetMetricPath(name string, hash uint64, labelNames []string, isAggr bool) string {
	agg := ""
	if isAggr {
		if len(labelNames) == 0 {
			agg = "agg/"
		} else {
			var namelessLabelNames []string
			for _, l := range labelNames {
				if l != config.PrometheusMetricNameAttribute {
					namelessLabelNames = append(namelessLabelNames, l)
				}
			}
			agg = fmt.Sprintf("agg/%s/", strings.Join(namelessLabelNames, ","))
		}
	}
	return fmt.Sprintf("%s%s%s_%x.%016x", p.path, agg, name, int(hash%uint64(p.GetHashingBuckets())), hash)
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

	var firstAttrTime int64
	if mint < p.startTime {
		firstAttrTime = p.startTime
	} else {
		firstAttrTime = p.startTime + ((mint-p.startTime)/p.chunkInterval)*p.chunkInterval
	}
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

func (p *DBPartition) ToMap() map[string]interface{} {
	attributes := make(map[string]interface{}, 5)
	attributes["aggregates"] = aggregate.AggregateMaskToString(p.AggrType())
	attributes["rollupTime"] = p.rollupTime
	attributes["chunkInterval"] = p.chunkInterval
	attributes["partitionInterval"] = p.partitionInterval
	attributes["retentionDays"] = p.retentionDays
	return attributes
}

// Convert a time in milliseconds to day and hour integers
func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	h := (t / 3600) % 24
	d := t / 3600 / 24
	return d, h
}
