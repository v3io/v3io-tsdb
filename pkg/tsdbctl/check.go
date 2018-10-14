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
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"time"
)

type checkCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	objPath        string
	attrs          []string
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

			if len(args) == 0 {
				return errors.New("The check command requires an item path.")
			}

			commandeer.objPath = args[0]

			// Initialize parameters
			return commandeer.check()
		},
	}

	cmd.Flags().StringSliceVarP(&commandeer.attrs, "attrs", "a", []string{},
		"[Required] An array of metric-item blob attribute names, as a\ncomma-separated list. For example: \"_v35\"; \"_v22,_v23\".")
	cmd.MarkFlagRequired("attrs")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *checkCommandeer) check() error {

	var err error
	var lset utils.Labels

	// Initialize the adapter
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	// Get metric data and metadata
	allAttrs := append(cc.attrs, "__name", "_name", "_lset", "_maxtime")
	container, tablePath := cc.rootCommandeer.adapter.GetContainer()
	objPath := fmt.Sprintf("%s/%s", tablePath, cc.objPath)
	input := v3io.GetItemInput{Path: objPath, AttributeNames: allAttrs}
	resp, err := container.Sync.GetItem(&input)
	if err != nil {
		return errors.Wrap(err, "GetItem failed.")
	}

	// Print the metric metadata
	item := resp.Output.(*v3io.GetItemOutput).Item
	objName, _ := item.GetFieldString("__name")
	metricName, _ := item.GetFieldString("_name")
	lsetString, _ := item.GetFieldString("_lset")
	maxtime, _ := item.GetFieldInt("_maxtime")
	fmt.Printf("Metric Item: %s,  %s {%s}  maxtime: %d\n", objName, metricName, lsetString, maxtime)

	// Decompress and print metrics
	for _, attr := range cc.attrs {

		values := item.GetField(attr)
		fmt.Println("Attr:", attr)

		if values != nil {
			bytes := values.([]byte)
			chunk, err := chunkenc.FromData(cc.rootCommandeer.logger, chunkenc.EncXOR, bytes, 0)
			if err != nil {
				cc.rootCommandeer.logger.ErrorWith("Error reading chunk buffer.", "Lset", lset, "err", err)
				return err
			} else {
				count := 0
				iter := chunk.Iterator()
				for iter.Next() {
					t, v := iter.At()
					tstr := time.Unix(int64(t/1000), 0).UTC().Format(time.RFC3339)
					fmt.Printf("Unix timestamp=%d, t=%s, v=%.4f \n", t, tstr, v)
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
		}
	}

	return nil
}
