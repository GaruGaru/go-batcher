package cond

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCond_Signal(t *testing.T) {
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		c.Wait(ctx)
		wg.Done()
	}()
	c.Signal()
	wg.Wait()
}

func TestCond_Broadcast(t *testing.T) {
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < 333; i++ {
		wg.Add(1)
		go func() {
			c.Wait(ctx)
			wg.Done()
		}()
	}
	c.Broadcast()
	wg.Wait()
}
