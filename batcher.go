package batcher

import (
	"context"
	"time"
)
import "golang.org/x/sync/errgroup"

type Batcher[I any] struct {
	// current batch using for item aggregation
	batch *BlockingBatch[I]
	stats *BatchStats
	// number of batch workers for processing using processFn
	workersCount int
	// process func for batch processing
	processFn func([]I) error
	errorFn   func(error)
	// EmitRule used to evaluate if batch dispatch is needed
	emitRule EmitRule
	// channel used to dispatch batches to workers for processing
	batchCh chan []I
	// func used for terminations
	cancelFn func()
	//
	workersGroup *errgroup.Group
}

// NewBatcher returns a new batcher given a list of Opt
func NewBatcher[I any](opts ...Opt[I]) *Batcher[I] {
	opt := &options[I]{
		maxSize:   100,
		workers:   1,
		processFn: nil,
		errorFn:   func(_ error) {},
		emitRule:  OnSizeReached(10),
	}

	for _, modifier := range opts {
		modifier.Apply(opt)
	}

	if opt.maxSize <= 0 {
		panic("batcher max size can't be <= 0")
	}

	if opt.workers <= 0 {
		panic("batcher workers can't be <= 0")
	}

	if opt.processFn == nil {
		panic("batcher process function must be defined")
	}

	return &Batcher[I]{
		batch:        NewBlockingQueue[I](opt.maxSize),
		stats:        &BatchStats{},
		processFn:    opt.processFn,
		workersCount: opt.workers,
		emitRule:     opt.emitRule,
		batchCh:      make(chan []I, opt.workers),
	}
}

// Accumulate N items, can block if the workers can't keep up with the processing
func (b *Batcher[I]) Accumulate(items ...I) {
	b.batch.Push(items...)
}

// Start initialize batcher and launch goroutines, blocks until Terminate is called or a non-recoverable error occurs
func (b *Batcher[I]) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	b.cancelFn = cancel

	workersGroup, ctx := errgroup.WithContext(ctx)

	workersGroup.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
				b.Emit(ctx)
				cancel()
				close(b.batchCh)
				return nil
			default:
				ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
				b.Emit(ctx)
				cancel()
			}
		}
	})

	for i := 0; i < b.workersCount; i++ {
		workersGroup.Go(func() error {
			return b.processBatchWorker(ctx)
		})
	}

	b.workersGroup = workersGroup
}

func (b *Batcher[I]) accumulateBatchWorker(ctx context.Context) error {
	return nil
}

func (b *Batcher[I]) processBatchWorker(ctx context.Context) error {
	for {
		select {
		case batch := <-b.batchCh:
			err := b.processFn(batch)
			if err != nil && b.errorFn != nil {
				b.errorFn(err)
			}
		case <-ctx.Done():
			for ba := range b.batchCh {
				err := b.processFn(ba)
				if err != nil && b.errorFn != nil {
					b.errorFn(err)
				}
			}
			return nil
		}
	}
}

func (b *Batcher[I]) emitIfNeeded(ctx context.Context) {
	b.stats.size = b.batch.Size()
	if b.emitRule.Check(*b.stats) {
		b.Emit(ctx)
	}
}

// Emit force emission of the current batch without evaluating the EmitRule
func (b *Batcher[I]) Emit(ctx context.Context) {
	batch := b.batch.PopAll(ctx)
	if len(batch) != 0 {
		b.batchCh <- batch
	}
}

func (b *Batcher[I]) Wait() error {
	err := b.workersGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (b *Batcher[I]) Terminate() {
	b.cancelFn()
}
