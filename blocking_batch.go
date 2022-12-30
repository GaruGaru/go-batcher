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
	empty   *cond.Cond
	fl      *sync.Mutex
	el      *sync.Mutex
}

func NewBlockingQueue[I any](size int) *BlockingBatch[I] {
	return &BlockingBatch[I]{
		items:   make([]I, size),
		maxSize: size,
		full:    cond.New(),
		empty:   cond.New(),
		fl:      &sync.Mutex{},
		el:      &sync.Mutex{},
	}
}

func (q *BlockingBatch[I]) Push(items ...I) {
	q.fl.Lock()
	for _, it := range items {
		// blocks if the max size has been reached
		for q.index == q.maxSize {
			q.fl.Unlock()
			q.full.Wait(context.TODO())
			q.fl.Lock()
		}
		q.items[q.index] = it
		q.index++
		if q.index >= q.maxSize {
			q.empty.Signal()
		}
	}
	q.fl.Unlock()
}

// PopAll Pops all the current items in the queue.
func (q *BlockingBatch[I]) PopAll(ctx context.Context) []I {
	q.waitFull(ctx)
	q.fl.Lock()
	defer q.fl.Unlock()
	cpy := make([]I, q.index)
	copy(cpy, q.items)
	q.index = 0
	q.full.Signal()
	return cpy
}

func (q *BlockingBatch[I]) waitFull(ctx context.Context) {
	q.el.Lock()
	defer q.el.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if q.index < q.maxSize {
				q.el.Unlock()
				q.empty.Wait(ctx)
				q.el.Lock()
			} else {
				return
			}
		}
	}
}

// Size return the current size of the queue using the internal index
// this method doesn't apply any concurrency control system the result should be treated as an estimate.
func (q *BlockingBatch[I]) Size() int {
	return q.index
}
