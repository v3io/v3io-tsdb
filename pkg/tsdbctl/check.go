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
	"github.com/spf13/cobra"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"time"
)

type checkCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	name           string
	lset           string
	attrs          []string
}

func newCheckCommandeer(rootCommandeer *RootCommandeer) *checkCommandeer {
	commandeer := &checkCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "check",
		Short: "check TSDB metric object",
		RunE: func(cmd *cobra.Command, args []string) error {

			if len(args) == 0 {
				return errors.New("add require metric name and/or labels")
			}

			commandeer.name = args[0]

			if len(args) > 1 {
				commandeer.lset = args[1]
			}

			// initialize params
			return commandeer.check()
		},
	}

	cmd.Flags().StringSliceVarP(&commandeer.attrs, "attrs", "a", []string{}, "attribute")


	commandeer.cmd = cmd

	return commandeer
}

func (cc *checkCommandeer) check() error {

	var err error
	var lset utils.Labels

	// initialize adapter
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	if lset, err = strToLabels(cc.name, cc.lset); err != nil {
		return err
	}

	container, path := cc.rootCommandeer.adapter.GetContainer()
	objPath := fmt.Sprintf("%s/0/%s.%016x", path , cc.name, lset.Hash())
	input := v3io.GetItemInput{ Path: objPath, AttributeNames: cc.attrs}
	resp, err := container.Sync.GetItem(&input)
	if err != nil {
		return errors.Wrap(err, "GetItems err")
	}

	item := resp.Output.(*v3io.GetItemOutput).Item
	for k, attr := range cc.attrs {

		values := item.GetField(attr)
		fmt.Println("Attr:", k)

		if values != nil {
			bytes := values.([]byte)
			chunk, err := chunkenc.FromData(chunkenc.EncXOR, bytes, 0)
			if err != nil {
				cc.rootCommandeer.logger.ErrorWith("Error reading chunk buffer", "Lset", lset, "err", err)
				return err
			} else {
				iter := chunk.Iterator()
				for iter.Next() {
					t, v := iter.At()
					tstr := time.Unix(int64(t/1000), 0).Format(time.RFC3339)
					fmt.Printf("unix=%d, t=%s, v=%.4f \n", t, tstr, v)
				}
			}
		}

	}

	return nil
}