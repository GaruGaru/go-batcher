package batcher

import (
	"context"
	"github.com/garugaru/go-batcher/cond"
	"sync"
)

// BlockingBatch implementation to create fixed size slices blocking append operations and avoiding OOMs
type BlockingBatch[I any] struct {
	items   []I
	index   int
	maxSize int
	full    *cond.Cond
	l       *sync.Mutex
}

func NewBlockingQueue[I any](size int) *BlockingBatch[I] {
	return &BlockingBatch[I]{
		items:   make([]I, size),
		maxSize: size,
		full:    cond.New(),
		l:       &sync.Mutex{},
	}
}

func (q *BlockingBatch[I]) Push(items ...I) {
	q.l.Lock()
	for _, it := range items {
		// blocks if the max size has been reached
		for q.index == q.maxSize {
			q.l.Unlock()
			q.full.Wait(context.TODO())
			q.l.Lock()
		}
		q.items[q.index] = it
		q.index++
	}
	q.l.Unlock()
}

// PopAll Pops all the current items in the queue.
func (q *BlockingBatch[I]) PopAll() []I {
	q.l.Lock()
	defer q.l.Unlock()
	cpy := make([]I, q.index)
	copy(cpy, q.items)
	q.index = 0
	q.full.Signal()
	return cpy
}

// Size return the current size of the queue using the internal index
// this method doesn't apply any concurrency control system the result should be treated as an estimate.
func (q *BlockingBatch[I]) Size() int {
	return q.index
}
