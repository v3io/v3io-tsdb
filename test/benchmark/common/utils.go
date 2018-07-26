package common

import (
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

func CreateTSDB(v3ioConfig *config.V3ioConfig, newTsdbPath string) error {
	dbcfg := config.DBPartConfig{
		DaysPerObj:     7,
		HrInChunk:      1,
		DefaultRollups: "*",
		RollupMin:      5,
	}
	return tsdb.CreateTSDB(v3ioConfig, &dbcfg)
}

func DeleteTSDB(adapter *tsdb.V3ioAdapter, deleteConf bool, force bool) {
	adapter.DeleteDB(deleteConf, force)
}
