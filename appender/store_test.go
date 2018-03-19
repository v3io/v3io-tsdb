package appender

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"testing"
)

func TestStore(t *testing.T) {
	store := chunkStore{}
	store.state = storeStateReady
	store.initChunk(0, 1520346654002)
	store.initChunk(1, 0)
	store.Append(1520346654002, 1.1)
	store.Append(1520346654002+3700000, 2.1)

	lset := labels.Labels{labels.Label{Name: "__name__", Value: "http_req"},
		labels.Label{Name: "method", Value: "post"}}

	exp, tid := store.WriteChunks(lset)
	println(exp, tid)
	store.ProcessWriteResp()
	store.Append(1520346654002+3702000, 2.1)
	exp, tid = store.WriteChunks(lset)
	println(exp, tid)
}
