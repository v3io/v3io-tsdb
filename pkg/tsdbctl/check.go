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

package tsdbctl

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

type checkCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	objPath        string
	metricName     string
	labels         utils.Labels
	attrs          []string
	from           string
	to             string
	last           string
}

func newCheckCommandeer(rootCommandeer *RootCommandeer) *checkCommandeer {
	commandeer := &checkCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:    "check <item-path>",
		Hidden: true,
		Short:  "Get information about a TSDB metric item",
		Long:   `Get information about a TSDB metric item.`,
		Example: `The examples assume that the endpoint of the web-gateway service, the login credentials, and
the name of the data container are configured in the default configuration file (` + config.DefaultConfigurationFileName + `)
- tsdbctl check 1538265600/memo_1.c6b54e7ce82c2c11 -t my_tsdb -a _v12

Arguments:
- <item-path> (string) [Required] Path to a metric item within the TSDB table.`,
		RunE: func(cmd *cobra.Command, args []string) error {

			numArgs := len(args)

			if numArgs == 0 {
				return errors.New("The check command requires a metric name.")
			}

			if strings.Contains(args[0], "/") {
				if len(commandeer.attrs) == 0 {
					return errors.New("-a (--attrs) is required when specifying a path")
				}
				commandeer.objPath = args[0]
				return commandeer.check()
			}

			commandeer.metricName = args[0]
			commandeer.labels = append(commandeer.labels, utils.Label{Name: "__name__", Value: commandeer.metricName})

			if numArgs > 1 {
				for _, labelStr := range strings.Split(args[1], ",") {
					splitLabelParts := strings.SplitN(labelStr, "=", 2)
					if len(splitLabelParts) < 2 {
						return errors.Errorf("failed to parse '%s' as a label", labelStr)
					}
					commandeer.labels = append(commandeer.labels, utils.Label{Name: splitLabelParts[0], Value: splitLabelParts[1]})
				}
			}

			// Initialize parameters
			return commandeer.check()
		},
	}

	cmd.Flags().StringSliceVarP(&commandeer.attrs, "attrs", "a", []string{},
		"An array of metric-item blob attribute names, as a\ncomma-separated list. For example: \"_v35\"; \"_v22,_v23\".")
	cmd.Flags().StringVarP(&commandeer.to, "end", "e", "",
		"End (maximum) time for the query, as a string containing an\nRFC 3339 time string, a Unix timestamp in milliseconds, or\na relative time of the format \"now\" or \"now-[0-9]+[mhd]\"\n(where 'm' = minutes, 'h' = hours, and 'd' = days).\nExamples: \"2018-09-26T14:10:20Z\"; \"1537971006000\";\n\"now-3h\"; \"now-7d\". (default \"now\")")
	cmd.Flags().StringVarP(&commandeer.from, "begin", "b", "",
		"Start (minimum) time for the query, as a string containing\nan RFC 3339 time, a Unix timestamp in milliseconds, a\nrelative time of the format \"now\" or \"now-[0-9]+[mhd]\"\n(where 'm' = minutes, 'h' = hours, and 'd' = days), or 0\nfor the earliest time. Examples: \"2016-01-02T15:34:26Z\";\n\"1451748866\"; \"now-90m\"; \"0\". (default = <end time> - 1h)")
	cmd.Flags().StringVarP(&commandeer.last, "last", "l", "",
		"Return data for the specified time period before the\ncurrent time, of the format \"[0-9]+[mhd]\" (where\n'm' = minutes, 'h' = hours, and 'd' = days>). When setting\nthis flag, don't set the -b|--begin or -e|--end flags.\nExamples: \"1h\"; \"15m\"; \"30d\" to return data for the last\n1 hour, 15 minutes, or 30 days.")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *checkCommandeer) check() error {

	var err error

	// Initialize the adapter
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	// Get metric data and metadata
	container, tablePath := cc.rootCommandeer.adapter.GetContainer()

	var respChans []chan *v3io.Response
	if cc.metricName == "" {
		respChan, err := cc.checkByPath(container, tablePath)
		if err != nil {
			return errors.Wrapf(err, "failed to check using object path '%s'", cc.objPath)
		}
		respChans = []chan *v3io.Response{respChan}
	} else {
		respChans, err = cc.checkByName(container, tablePath)
		if err != nil {
			return errors.Wrapf(err, "failed to check using metric name '%s' and labels '%v'", cc.metricName, cc.labels)
		}
	}

	for _, respChan := range respChans {
		resp := <-respChan
		if resp.Error != nil {
			return errors.Wrap(resp.Error, "GetItem returned with an error")
		}
		err = cc.printResponse(resp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cc *checkCommandeer) checkByPath(container *v3io.Container, tablePath string) (chan *v3io.Response, error) {
	var err error
	respChan := make(chan *v3io.Response, 1)
	objPath := path.Join("/", tablePath, cc.objPath)
	allAttrs := append(cc.attrs, "__name", "_name", "_lset", "_maxtime")
	input := v3io.GetItemInput{Path: objPath, AttributeNames: allAttrs}
	_, err = container.GetItem(&input, nil, respChan)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send GetItem request")
	}
	return respChan, nil
}

func (cc *checkCommandeer) checkByName(container *v3io.Container, tablePath string) ([]chan *v3io.Response, error) {

	var err error

	schema, err := getSchema(cc.rootCommandeer.v3iocfg, container)
	if err != nil {
		return nil, err
	}
	numBuckets := schema.TableSchemaInfo.ShardingBucketsCount
	hash := cc.labels.Hash()
	var from, to int64
	if cc.from == "" {
		from = 0
	} else {
		from, err = utils.Str2unixTime(cc.from)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse start time '%s'", cc.from)
		}
	}
	if cc.to == "" {
		to = math.MaxInt64
	} else {
		to, err = utils.Str2unixTime(cc.to)
		return nil, errors.Wrapf(err, "failed to parse end time '%s'", cc.to)
	}
	partitionInterval, _ := utils.Str2duration(schema.PartitionSchemaInfo.PartitionerInterval)
	var respChans []chan *v3io.Response
	for _, partition := range schema.Partitions {
		partitionEndTime := partition.StartTime + partitionInterval
		if partitionEndTime >= from && partition.StartTime <= to {
			respChan := make(chan *v3io.Response, 1)
			respChans = append(respChans, respChan)
			objName := fmt.Sprintf("%s_%d.%x", cc.metricName, hash%uint64(numBuckets), hash)
			objPath := path.Join(tablePath, strconv.FormatInt(partition.StartTime/1000, 10), objName)
			input := v3io.GetItemInput{Path: objPath, AttributeNames: []string{"**"}}
			_, err := container.GetItem(&input, nil, respChan)
			if err != nil {
				return nil, errors.Wrap(err, "failed to send GetItem request")
			}
		}
	}
	return respChans, nil
}

func (cc *checkCommandeer) printResponse(resp *v3io.Response) error {

	// Print the metric metadata
	item := resp.Output.(*v3io.GetItemOutput).Item
	objPath := resp.Request().Input.(*v3io.GetItemInput).Path
	metricName, _ := item.GetFieldString("_name")
	lsetString, _ := item.GetFieldString("_lset")
	maxtime, _ := item.GetFieldInt("_maxtime")
	fmt.Printf("Metric Item: %s,  %s{%s}  maxtime: %d\n", objPath, metricName, lsetString, maxtime)

	// Decompress and print metrics
	for attr, values := range item {
		if strings.HasPrefix(attr, "_v") {
			fmt.Println("\tAttr:", attr)

			if values != nil {
				bytes := values.([]byte)
				if strings.HasPrefix(attr, "_v_") {
					cc.printArrays(attr, bytes)
				} else {
					err := cc.printValues(bytes)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (cc *checkCommandeer) printArrays(attr string, bytes []byte) {
	var result strings.Builder

	result.WriteString("[")
	for i, val := range utils.AsInt64Array(bytes) {
		if i != 0 {
			result.WriteString(",")
		}
		// Count arrays are integers, while all other aggregates are floats
		if strings.Contains(attr, "count") {
			result.WriteString(fmt.Sprintf("%v", val))
		} else {
			result.WriteString(fmt.Sprintf("%v", math.Float64frombits(val)))
		}
	}
	result.WriteString("]")
	fmt.Println(result.String())
}
func (cc *checkCommandeer) printValues(bytes []byte) error {
	chunk, err := chunkenc.FromData(cc.rootCommandeer.logger, chunkenc.EncXOR, bytes, 0)
	if err != nil {
		cc.rootCommandeer.logger.ErrorWith("Error reading chunk buffer.", "err", err)
		return err
	} else {
		count := 0
		iter := chunk.Iterator()
		for iter.Next() {
			t, v := iter.At()
			tstr := time.Unix(int64(t/1000), 0).UTC().Format(time.RFC3339)
			fmt.Printf("\t\tUnix timestamp=%d, t=%s, v=%.4f\n", t, tstr, v)
			count++
		}
		if iter.Err() != nil {
			return errors.Wrap(iter.Err(), "Failed to read the iterator.")
		}

		compressionRatio := 0.0
		bytesCount := len(bytes)
		if count > 0 {
			compressionRatio = float64(bytesCount) / float64(count)
		}
		fmt.Printf("Total size=%d, Count=%d, Compression ratio=%.2f\n",
			bytesCount, count, compressionRatio)
	}
	return nil
}

func getSchema(cfg *config.V3ioConfig, container *v3io.Container) (*config.Schema, error) {
	fullpath := path.Join(cfg.WebApiEndpoint, cfg.Container, cfg.TablePath)
	resp, err := container.Sync.GetObject(&v3io.GetObjectInput{Path: path.Join(cfg.TablePath, config.SchemaConfigFileName)})
	if err != nil {
		if utils.IsNotExistsError(err) {
			return nil, errors.Errorf("No TSDB schema file found at '%s'.", fullpath)
		} else {
			return nil, errors.Wrapf(err, "Failed to read a TSDB schema from '%s'.", fullpath)
		}

	}

	tableSchema := &config.Schema{}
	err = json.Unmarshal(resp.Body(), tableSchema)
	if err != nil {
		return nil, err
	}
	return tableSchema, nil
}
