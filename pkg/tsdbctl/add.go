package tsdbctl

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"strconv"
	"strings"
	"time"
)

type addCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	lset           string
	tArr           string
	vArr           string
}

func newAddCommandeer(rootCommandeer *RootCommandeer) *addCommandeer {
	commandeer := &addCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "add metric",
		Aliases: []string{"put"},
		Short:   "add samples to metric",
		RunE: func(cmd *cobra.Command, args []string) error {

			// if we got positional arguments
			if len(args) != 1 {
				return errors.New("add require metric labels")
			}

			commandeer.lset = args[0]

			// initialize params and adapter
			if err := rootCommandeer.initialize(); err != nil {
				return err
			}

			if err := rootCommandeer.startAdapter(); err != nil {
				return err
			}

			return commandeer.add()

		},
	}

	cmd.Flags().StringVarP(&commandeer.tArr, "times", "t", "", "time array, comma separated")
	cmd.Flags().StringVarP(&commandeer.vArr, "values", "d", "", "values array, comma separated")

	commandeer.cmd = cmd

	return commandeer
}

func (ac *addCommandeer) add() error {

	lset := utils.Labels{}
	fmt.Println("str:", ac.lset)
	//err := json.Unmarshal([]byte(`{"__name__":"cpu","os":"win","node":"xyz123"}`), &lset)
	err := json.Unmarshal([]byte(ac.lset), &lset)
	if err != nil {
		return errors.Wrap(err, "cant unmarshal labels")
	}

	if ac.vArr == "" {
		return errors.Wrap(err, "must have at least one value")
	}

	tlist := strings.Split(ac.tArr, ",")
	vlist := strings.Split(ac.vArr, ",")

	if ac.tArr == "" && len(vlist) > 1 {
		return errors.Wrap(err, "time array must be provided when using a value array")
	}

	if ac.tArr != "" && len(tlist) != len(vlist) {
		return errors.Wrap(err, "time and value arrays must have the same number of elements")
	}

	tarray := []int64{}
	varray := []float64{}

	for i := 0; i < len(vlist); i++ {

		var v float64
		var err error
		if vlist[i] == "NaN" {
			v = math.NaN()
		} else {
			v, err = strconv.ParseFloat(vlist[i], 64)
			if err != nil {
				return errors.Wrap(err, "not a valid float value")
			}
		}

		varray = append(varray, v)
	}

	if ac.tArr == "" {
		tarray = append(tarray, int64(time.Now().Unix()*1000))
	} else {
		for i := 0; i < len(vlist); i++ {
			t, err := strconv.Atoi(tlist[i])
			if err != nil {
				return errors.Wrap(err, "not a valid (unix mili) time")
			}
			tarray = append(tarray, int64(t))
		}
	}

	fmt.Println("add:", lset, tarray, varray)
	append, err := ac.rootCommandeer.adapter.Appender()
	ref, err := append.Add(lset, tarray[0], varray[0])
	if err != nil {
		return errors.Wrap(err, "failed to Add")
	}

	for i := 1; i < len(varray); i++ {
		err := append.AddFast(lset, ref, tarray[i], varray[i])
		if err != nil {
			return errors.Wrap(err, "failed to AddFast")
		}
	}

	return append.WaitForReady(ref)
}
