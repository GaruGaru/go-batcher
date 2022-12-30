package batcher

import (
	"context"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"sync"
	"testing"
	"time"
)

func TestBlockingQueueConcurrency_PopAll_Empty(t *testing.T) {
	bq := NewBlockingQueue[string](1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	bq.PopAll(ctx)
	cancel()
}

func TestBlockingQueueConcurrency_Push_PopAll(t *testing.T) {
	bq := NewBlockingQueue[string](1)
	bq.Push("a")
	items := bq.PopAll(context.TODO())
	require.Equal(t, []string{"a"}, items)
}

func TestBlockingQueueConcurrency_WithConcurrency(t *testing.T) {
	var (
		publishers = 321
		consumers  = 123
		items      = 2234
		queueSize  = 33
	)

	bq := NewBlockingQueue[int](queueSize)

	ctx := context.Background()
	pubGroup, ctx := errgroup.WithContext(ctx)
	consGroup, _ := errgroup.WithContext(ctx)

	var publishing = true
	for i := 0; i < publishers; i++ {
		pubGroup.Go(func() error {
			for i := 0; i < items; i++ {
				bq.Push(i)
			}
			return nil
		})
	}

	popped := make([]int, 0)
	cLock := &sync.Mutex{}
	for i := 0; i < consumers; i++ {
		consGroup.Go(func() error {
			for publishing || bq.Size() > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				items := bq.PopAll(ctx)
				if items == nil {
					cancel()
					continue
				}
				cLock.Lock()
				popped = append(popped, items...)
				cLock.Unlock()
				cancel()
			}

			return nil
		})
	}

	require.NoError(t, pubGroup.Wait())
	publishing = false
	require.NoError(t, consGroup.Wait())

	require.Len(t, popped, items*publishers)
	require.Equal(t, 0, bq.Size())
}

func TestPopAll_Full(t *testing.T) {
	bq := NewBlockingQueue[int](123)
	for i := 0; i < 100; i++ {
		bq.Push(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	bq.PopAll(ctx)
	cancel()
}

func BenchmarkBlockingQueue(b *testing.B) {
	bq := NewBlockingQueue[int](123)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 123; i++ {
			bq.Push(i)
		}
		bq.PopAll(context.TODO())
	}
}
