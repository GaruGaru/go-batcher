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
	// channel used to dispatch items to batch
	itemsCh chan []I
	// func used for terminations
	cancelFn func()
	//
	workersGroup    *errgroup.Group
	accumulateGroup *errgroup.Group
	dispatcherGroup *errgroup.Group
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
		itemsCh:      make(chan []I, opt.workers),
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

	accumulatorGroup, ctx := errgroup.WithContext(ctx)
	dispatcherGroup, ctx := errgroup.WithContext(ctx)
	workersGroup, ctx := errgroup.WithContext(ctx)

	dispatcherGroup.Go(func() error {
		return b.dispatchWorker(ctx, 100*time.Millisecond)
	})

	accumulatorGroup.Go(func() error {
		err := b.accumulateBatchWorker(ctx)
		b.Emit()
		close(b.batchCh)
		return err
	})

	for i := 0; i < b.workersCount; i++ {
		workersGroup.Go(func() error {
			return b.processBatchWorker(ctx)
		})
	}

	b.accumulateGroup = accumulatorGroup
	b.workersGroup = workersGroup
	b.dispatcherGroup = dispatcherGroup
}

func (b *Batcher[I]) dispatchWorker(ctx context.Context, checkDelay time.Duration) error {
	tk := time.NewTicker(checkDelay)
	for {
		select {
		case <-tk.C:
			b.emitIfNeeded()
		case <-ctx.Done():
			tk.Stop()
			return nil
		}
	}
}

func (b *Batcher[I]) accumulateBatchWorker(ctx context.Context) error {
	for {
		select {
		case it, hasItems := <-b.itemsCh:
			if !hasItems {
				continue
			}
			b.batch.Push(it...)
		case <-ctx.Done():
			for it := range b.itemsCh {
				b.batch.Push(it...)
			}
			return nil
		}
	}
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

func (b *Batcher[I]) emitIfNeeded() {
	b.stats.size = b.batch.Size()
	if b.emitRule.Check(*b.stats) {
		b.Emit()
	}
}

// Emit force emission of the current batch without evaluating the EmitRule
func (b *Batcher[I]) Emit() {
	batch := b.batch.PopAll(context.TODO())
	if len(batch) != 0 {
		b.batchCh <- batch
	}
}

func (b *Batcher[I]) Wait() error {
	err := b.accumulateGroup.Wait()
	if err != nil {
		return err
	}
	err = b.workersGroup.Wait()
	if err != nil {
		return err
	}
	err = b.dispatcherGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (b *Batcher[I]) Terminate() {
	close(b.itemsCh)
	b.cancelFn()
}
