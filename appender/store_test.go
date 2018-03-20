package appender

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	fmt.Println(time.Now().Unix())
	store := NewChunkStore()
	store.state = storeStateReady
	store.Append(1521531613002, 1.1)
	store.Append(1521531613002+3500000, 2.1)

	lset := labels.Labels{labels.Label{Name: "__name__", Value: "http_req"},
		labels.Label{Name: "method", Value: "post"}}

	exp, tid := store.WriteChunks(lset)
	println(exp, tid)
	store.ProcessWriteResp()
	store.Append(1521531613002+6702000, 2.1)
	exp, tid = store.WriteChunks(lset)
	println(exp, tid)
}
