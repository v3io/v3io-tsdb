package appender

import (
	"fmt"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/chunkenc"
	"github.com/v3io/v3io-tsdb/utils"
)

var MaxArraySize = 1024

const MAX_LATE_WRITE = 59 * 3600 * 1000 // max late arrival of 59min

func NewChunkStore() *chunkStore {
	store := chunkStore{}
	store.chunks[0] = &attrAppender{}
	store.chunks[1] = &attrAppender{}
	return &store
}

// store latest + previous chunks and their state
type chunkStore struct {
	state    storeState
	curChunk int
	pending  []pendingData
	chunks   [2]*attrAppender
}

// chunk appender
type attrAppender struct {
	appender   chunkenc.Appender
	updMarker  int
	updCount   int
	lastT      int64
	mint, maxt int64 // TODO: maxt can be calculated value or #hours to save space
	writing    bool
}

func (a *attrAppender) clear() {
	a.updCount = 0
	a.updMarker = 0
	a.lastT = 0
	a.writing = false
}

// TODO: change appender from float to interface (allow map[str]interface cols)
func (a *attrAppender) appendAttr(t int64, v interface{}) {
	a.appender.Append(t, v.(float64))
	if t > a.lastT {
		a.lastT = t
	}
}

type pendingData struct {
	t int64
	v interface{}
}

type storeState uint8

const (
	storeStateInit   storeState = 0
	storeStateGet    storeState = 1 // Getting old state from storage
	storeStateReady  storeState = 2 // Ready to update
	storeStateUpdate storeState = 3 // Update/write in progress
	storeStateSort   storeState = 3 // TBD sort chunk(s) in case of late arrivals
)

func (cs *chunkStore) IsReady() bool {
	return cs.state == storeStateReady
}

func (cs *chunkStore) GetState() storeState {
	return cs.state
}

func (cs *chunkStore) SetState(state storeState) {
	cs.state = state
}

// return how many un-commited writes
func (cs *chunkStore) UpdatesBehind() int {
	updatesBehind := len(cs.pending)
	for _, chunk := range cs.chunks {
		if chunk.appender != nil {
			updatesBehind += chunk.appender.Chunk().NumSamples() - chunk.updCount
		}
	}
	return updatesBehind
}

// Read (Async) the current chunk state and data from the storage
func (cs *chunkStore) GetChunksState(mc *MetricsCache, t int64) {

	// clac table mint
	// Get record meta + relevant attr
	// return get resp id & err

	cs.state = storeStateGet

}

// Process the GetItem response from the storage and initialize or restore the current chunk
func (cs *chunkStore) ProcessGetResp(resp *v3io.Response) {
	// init chunk from resp (attr)
	// append last t/v into the chunk and clear pending
}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v interface{}) error {

	// if it was just created and waiting to get the state we append to temporary list
	if cs.state == storeStateGet || cs.state == storeStateInit {
		cs.pending = append(cs.pending, pendingData{t: t, v: v})
		return nil
	}

	cur := cs.chunks[cs.curChunk]
	if t >= cur.mint && t < cur.maxt {
		// Append t/v to current chunk
		cur.appendAttr(t, v)
		return nil
	}

	if t >= cur.maxt {
		// advance cur chunk
		cur = cs.chunks[cs.curChunk^1]

		// avoid append if there are still writes to old chunk
		if cur.writing {
			return fmt.Errorf("Error, append beyound cur chunk while IO in flight to prev", t)
		}

		chunk := chunkenc.NewXORChunk() // TODO: init based on schema, use init function
		app, err := chunk.Appender()
		if err != nil {
			return err
		}
		cur.clear()
		cur.appender = app
		cur.mint, cur.maxt = utils.Time2MinMax(0, t) // TODO: dpo from config
		cur.appendAttr(t, v)
		cs.curChunk = cs.curChunk ^ 1

		return nil
	}

	// write to an older chunk

	prev := cs.chunks[cs.curChunk^1]
	// delayed Appends only allowed to previous chunk or within allowed window
	if t >= prev.mint && t < prev.maxt && t > cur.lastT-MAX_LATE_WRITE {
		// Append t/v to previous chunk
		prev.appendAttr(t, v)
		return nil
	}

	// if (mint - t) in allowed window may need to advance next (assume prev is not the one right before cur)

	return nil
}

// Write pending data of the current or previous chunk to the storage
func (cs *chunkStore) WriteChunks(mc *MetricsCache, metric *MetricState) error {

	tableId := -1
	expr := ""

	if cs.state == storeStateGet {
		// cannot write if restoring chunk state is in progress
		return nil
	}

	// TODO: maybe have a bool to indicate if we have new appends vs checking all the chunks..

	notInitialized := false //TODO: init depend on get

	for i := 1; i < 3; i++ {
		chunk := cs.chunks[cs.curChunk^(i&1)]
		tid := utils.Time2TableID(chunk.mint)

		// write to a single table partition at a time, updated to 2nd partition will wait for next round
		if chunk.appender != nil && (tableId == -1 || tableId == tid) {

			samples := chunk.appender.Chunk().NumSamples()
			if samples > chunk.updCount {
				tableId = tid
				cs.state = storeStateUpdate
				meta, offsetByte, b := chunk.appender.Chunk().GetChunkBuffer()
				chunk.updMarker = ((offsetByte + len(b) - 1) / 8) * 8
				chunk.updCount = samples
				chunk.writing = true

				notInitialized = (offsetByte == 0)
				expr = expr + chunkbuf2Expr(offsetByte, meta, b, chunk.mint, chunk.lastT)

			}
		}
	}

	if notInitialized { // TODO: not correct, need to init once per metric/partition, maybe use cond expressions
		lblexpr := ""
		for _, lbl := range metric.Lset {
			if lbl.Name != "__name__" {
				lblexpr = lblexpr + fmt.Sprintf("%s='%s'; ", lbl.Name, lbl.Value)
			} else {
				lblexpr = lblexpr + fmt.Sprintf("_name='%s'; ", lbl.Value)
			}
		}
		expr = lblexpr + fmt.Sprintf("_lset='%s'; _meta_v=init_array(%d,'int'); ",
			metric.key, 24) + expr // TODO: compute meta arr size
	}

	path := fmt.Sprintf("%s/%s.%d", mc.cfg.Path, metric.name, metric.hash) // TODO: use TableID
	request, err := mc.container.UpdateItem(&v3io.UpdateItemInput{Path: path, Expression: &expr}, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("UpdateItem Failed", "err", err)
		return err
	}

	mc.logger.DebugWith("updateMetric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)
	mc.rmapMtx.Lock()
	defer mc.rmapMtx.Unlock()
	mc.requestsMap[request.ID] = metric

	return nil
}

// Process the response for the chunk update request
func (cs *chunkStore) ProcessWriteResp() {

	for _, chunk := range cs.chunks {
		if chunk.writing {
			chunk.appender.Chunk().MoveOffset(uint16(chunk.updMarker))
			chunk.writing = false
		}
	}

	cs.state = storeStateReady

	for _, data := range cs.pending {
		cs.Append(data.t, data.v)
	}

}

func chunkbuf2Expr(offsetByte int, meta uint64, bytes []byte, mint, maxt int64) string {

	expr := ""
	offset := offsetByte / 8
	ui := chunkenc.ToUint64(bytes)
	idx, hrInChunk := utils.TimeToChunkId(0, mint) // TODO: add DaysPerObj from part manager
	attr := utils.ChunkID2Attr("v", idx, hrInChunk)

	if offsetByte == 0 {
		expr = expr + fmt.Sprintf("%s=init_array(%d,'int'); ", attr, MaxArraySize)
	}

	expr = expr + fmt.Sprintf("_meta_v[%d]=%d; ", idx, meta) // TODO: meta name as col variable
	for i := 0; i < len(ui); i++ {
		expr = expr + fmt.Sprintf("%s[%d]=%d; ", attr, offset, int64(ui[i]))
		offset++
	}
	expr += fmt.Sprintf("_maxtime=%d", maxt) // TODO: use max() expr

	return expr
}
