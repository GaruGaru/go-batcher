package batcher

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBatcher_Emit(t *testing.T) {
	var (
		iterations = 100
		itemsCount = 112
	)
	for it := 0; it < iterations; it++ {
		tp := newTestableProcessFn[string]()
		b := NewBatcher[string](
			Process(tp.process),
			MaxSize[string](itemsCount),
			Emit[string](
				OnSizeReached(3),
				Every(10*time.Millisecond),
			),
		)

		b.Start(context.TODO())
		for i := 0; i < itemsCount; i++ {
			b.Accumulate(fmt.Sprintf("%d", i))
		}
		b.Terminate()
		require.NoError(t, b.Wait())

		processedItems := 0
		for _, c := range tp.Calls {
			processedItems += len(c)
		}
		assert.Equal(t, itemsCount, processedItems, tp.Calls)
	}
}

func BenchmarkNewBatcher(b *testing.B) {
	var itemsCount = 123
	tp := newTestableProcessFn[int]()

	for i := 0; i < b.N; i++ {
		batcher := NewBatcher[int](
			Process(tp.process),
			MaxSize[int](itemsCount),
			Emit[int](
				OnSizeReached(3),
				Every(10*time.Millisecond),
			),
		)

		b.ReportAllocs()
		batcher.Start(context.TODO())
		for i := 0; i < itemsCount; i++ {
			batcher.Accumulate(i)
		}

		batcher.Terminate()
		require.NoError(b, batcher.Wait())
	}
}
