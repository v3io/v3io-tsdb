package appender

import (
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/chunkenc"
)

var MaxUpdatesBehind = 3

type chunkStore struct {
	uncommited    bool
	updatesBehind int
	state         storeState
	curAppender   chunkenc.Appender
	prevAppender  chunkenc.Appender
	pending       []pendingData
}

type pendingData struct {
	t int64
	v float64
}

type storeState uint8

const (
	storeStateInit    storeState = 0
	storeStateGet     storeState = 1
	storeStateReady   storeState = 2
	storeStateUpdCur  storeState = 3
	storeStateUpdPrev storeState = 4
)

func (cs *chunkStore) hasUncommited() bool {
	return cs.uncommited
}

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

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v float64) {

	cs.updatesBehind++

	// if it was just created and waiting to get the state we append to temporary list
	if cs.state == storeStateGet || cs.state == storeStateInit {
		cs.pending = append(cs.pending, pendingData{t: t, v: v})
		return
	}

	mint, maxt := cs.curAppender.Chunk().TimeRange()
	if t >= mint && t < maxt {
		// Append t/v to current chunk
	}

	if t >= maxt {
		// need to advance to a new chunk

		// prev chunk = cur chunk
		// init cur chunk, append t/v to it
		// note: new chunk row/attr may already be initialized for some reason, can ignore or try to Get old one first
	}

	// write to an older chunk

	if cs.prevAppender == nil {
		// No older chunk, need to init one if we are in the allowed time window or ignore if its too old
		// create prev chunk, add t/v to it

		// for now we ignore old updates in that case
		return
	}

	prevMint, prevMaxt := cs.prevAppender.Chunk().TimeRange()

	if t >= prevMint && t < prevMaxt {
		// Append t/v to previous chunk
	}

	// if (mint - t) in allowed window may need to advance prevChunk (assume prev is not the one right before cur)
	// if prev has pending data need to wait until it was commited to create a new prev

}

// Process the GetItem response from the storage and initialize or restore the current chunk
func (cs *chunkStore) processGetResp(resp *v3io.Response) {
	// init chunk from resp (attr)
	// append last t/v into the chunk and clear pending
}

// Write pending data of the current or previous chunk to the storage
func (cs *chunkStore) WriteChunks() {

}

// Process the response for the chunk update request
func (cs *chunkStore) processWriteResp() {

	// see if we updated cur/prev from the state
	// advance pointers and restore state & samples behind values

}
