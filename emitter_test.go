package batcher

import (
	"context"
	"testing"
	"time"
)

func TestEmitter_MultiRule(t *testing.T) {
	rule := NewMultiEmitRule([]EmitRule{
		OnSizeReached(1),
		Every(10 * time.Second),
	})

	time.AfterFunc(100*time.Millisecond, func() {
		rule.Check(BatchStats{
			size: 13,
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	select {
	case <-rule.Emit():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
