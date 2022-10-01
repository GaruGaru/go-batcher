package batcher

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEmitter_MultiRule(t *testing.T) {
	rule := MultiEmitRule{
		Rules: []EmitRule{
			OnSizeReached(1),
			Every(10 * time.Second),
		},
	}

	shouldEmit := rule.Check(BatchStats{
		size: 2,
	})

	require.True(t, shouldEmit)
}
