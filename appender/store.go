package appender

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/chunkenc"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/utils"
	"math"
)

var MaxUpdatesBehind = 3
var MaxArraySize = 1024
var Config *config.TsdbConfig

type chunkStore struct {
	updatesBehind int
	state         storeState
	//curAppender     chunkenc.Appender
	//curUpdMarker    int
	//curUpdCount     int
	//prevAppender    chunkenc.Appender
	//prevUpdMarker   int
	//prevUpdCount    int
	curChunk int
	pending  []pendingData
	chunks   [2]*attrAppender
}

type attrAppender struct {
	appender   chunkenc.Appender
	updMarker  int
	updCount   int
	mint, maxt int64
	writing    bool
}

func (a attrAppender) clear() {
	a.updCount = 0
	a.updMarker = 0
	a.writing = false
}

type pendingData struct {
	t int64
	v float64
}

type storeState uint8

const (
	storeStateInit   storeState = 0
	storeStateGet    storeState = 1
	storeStateReady  storeState = 2
	storeStateUpdate storeState = 3
	//storeStateUpdCur  storeState = 3
	//storeStateUpdPrev storeState = 4
	//storeStateUpdBoth storeState = 5
)

func (cs *chunkStore) IsReady() bool {
	return cs.state == storeStateReady
}

func (cs *chunkStore) IsBlocked() bool {
	return cs.updatesBehind > MaxUpdatesBehind
}

// Read (Async) the current chunk state and data from the storage
func (cs *chunkStore) GetChunksState(mc *MetricsCache, t int64) {

	// clac table mint
	// Get record meta + relevant attr
	// return get resp id & err

	cs.state = storeStateGet

}

func (cs *chunkStore) initChunk(idx int, t int64) error {
	chunk := chunkenc.NewXORChunk(0, math.MaxInt64) // TODO: calc new mint/maxt
	app, err := chunk.Appender()
	if err != nil {
		return err
	}

	cs.chunks[idx] = &attrAppender{appender: app}
	cs.chunks[idx].mint, cs.chunks[idx].maxt = utils.Time2MinMax(0, t) // TODO: dpo from config

	return nil
}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v float64) error {

	cs.updatesBehind++

	// if it was just created and waiting to get the state we append to temporary list
	if cs.state == storeStateGet || cs.state == storeStateInit {
		cs.pending = append(cs.pending, pendingData{t: t, v: v})
		return nil
	}

	cur := cs.chunks[cs.curChunk]
	if t >= cur.mint && t < cur.maxt {
		// Append t/v to current chunk
		cur.appender.Append(t, v)
		return nil
	}

	next := cs.chunks[cs.curChunk^1]
	if t >= cur.maxt {

		// avoid append if there are still writes to prev chunk
		if next.writing {
			fmt.Println("Error, append to new chunk while writing to prev", t)
			//	cs.pending = append(cs.pending, pendingData{t: t, v: v})
			//	return
		}

		chunk := chunkenc.NewXORChunk(0, math.MaxInt64) // TODO: calc new mint/maxt
		app, err := chunk.Appender()
		if err != nil {
			return err
		}
		next.clear()
		next.appender = app
		next.mint, next.maxt = utils.Time2MinMax(0, t) // TODO: dpo from config
		fmt.Println("times:", t, next.mint, next.maxt, v)
		next.appender.Append(t, v)
		cs.curChunk = cs.curChunk ^ 1

		return nil

		// need to advance to a new chunk

		// prev chunk = cur chunk
		// init cur chunk, append t/v to it
		// note: new chunk row/attr may already be initialized for some reason, can ignore or try to Get old one first
	}

	// write to an older chunk

	if t >= next.mint && t < next.maxt {
		// Append t/v to previous chunk
		next.appender.Append(t, v)
		return nil
	}

	// if (mint - t) in allowed window may need to advance next (assume prev is not the one right before cur)
	// if prev has pending data need to wait until it was commited to create a new prev

	return nil
}

// Process the GetItem response from the storage and initialize or restore the current chunk
func (cs *chunkStore) processGetResp(resp *v3io.Response) {
	// init chunk from resp (attr)
	// append last t/v into the chunk and clear pending
}

// Write pending data of the current or previous chunk to the storage
func (cs *chunkStore) WriteChunks(lset labels.Labels) (int, string) {

	tableId := -1
	expr := ""

	if cs.state == storeStateGet {
		return tableId, expr
	}

	isInitialized := false

	for i := 1; i < 3; i++ {
		chunk := cs.chunks[cs.curChunk^(i&1)]
		tid := utils.Time2TableID(chunk.mint)

		if chunk.appender != nil && (tableId == -1 || tableId == tid) {

			samples := chunk.appender.Chunk().NumSamples()
			if samples > chunk.updCount {
				tableId = tid
				cs.state = storeStateUpdate
				meta, offsetByte, b := chunk.appender.Chunk().GetChunkBuffer()
				chunk.updMarker = ((offsetByte + len(b) - 1) / 8) * 8
				chunk.updCount = samples
				chunk.writing = true

				isInitialized = (offsetByte > 0)
				expr = expr + Chunkbuf2Expr(offsetByte, meta, b, chunk.mint)

			}
		}
	}

	/*
		if cs.prevAppender != nil && (cs.prevUpdCount > cs.prevAppender.Chunk().NumSamples()) {
			mint, _ := cs.prevAppender.Chunk().TimeRange()
			tableId = utils.Time2TableID(mint)

			cs.state = storeStateUpdPrev
			meta, offsetByte, b := cs.prevAppender.Chunk().GetChunkBuffer()
			cs.prevUpdMarker = ((offsetByte + len(b) - 1) / 8) * 8
			cs.prevUpdCount = cs.prevAppender.Chunk().NumSamples()
			isInitialized = (offsetByte > 0)
			expr = expr + Chunkbuf2Expr(offsetByte, meta, b, mint)

		}

		if cs.curAppender != nil && (cs.curUpdCount > cs.curAppender.Chunk().NumSamples()) {
			mint, _ := cs.prevAppender.Chunk().TimeRange()
			curTableId := utils.Time2TableID(mint)

			if tableId == -1 || tableId == curTableId {

				if tableId == -1 {
					cs.state = storeStateUpdCur
					tableId = curTableId
				} else {
					cs.state = storeStateUpdBoth
				}

				meta, offsetByte, b := cs.curAppender.Chunk().GetChunkBuffer()
				cs.curUpdMarker = ((offsetByte + len(b) - 1) / 8) * 8
				cs.curUpdCount = cs.curAppender.Chunk().NumSamples()
				isInitialized = (offsetByte > 0)
				expr = expr + Chunkbuf2Expr(offsetByte, meta, b, mint)


			}

		}
	*/

	if !isInitialized {
		lblexpr := ""
		for _, lbl := range lset {
			if lbl.Name != "__name__" {
				lblexpr = lblexpr + fmt.Sprintf("%s='%s'; ", lbl.Name, lbl.Value)
			} else {
				lblexpr = lblexpr + fmt.Sprintf("_name='%s'; ", lbl.Value)
			}
		}
		expr = lblexpr + expr
	}

	return tableId, expr

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

func Chunkbuf2Expr(offsetByte int, meta uint64, bytes []byte, mint int64) string {

	expr := ""
	offset := offsetByte / 8
	ui := chunkenc.ToUint64(bytes)
	attr := utils.TimeToChunkId(0, mint) // TODO: add DaysPerObj from config

	if offsetByte == 0 {
		expr = expr + fmt.Sprintf("%04d=init_array(%d,'int'); ", attr, MaxArraySize)
	}

	expr = expr + fmt.Sprintf("%04d[0]=%d; ", attr, meta)
	for i := 0; i < len(ui); i++ {
		offset++
		expr = expr + fmt.Sprintf("%04d[%d]=%d; ", attr, offset, int64(ui[i]))
	}

	return expr
}
