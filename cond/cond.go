package cond

import (
	"context"
	"sync"
)

// Cond a sync.Cond alternative which supports context.Context
type Cond struct {
	c  chan struct{}
	cl *sync.Mutex
}

func New() *Cond {
	return &Cond{
		c:  make(chan struct{}),
		cl: &sync.Mutex{},
	}
}

func (c *Cond) Wait(ctx context.Context) {
	select {
	case <-c.c:
		return
	case <-ctx.Done():
		return
	}
}

func (c *Cond) Signal() {
	select {
	case c.c <- struct{}{}:
	default:
		return
	}
}

func (c *Cond) Broadcast() {
	c.cl.Lock()
	close(c.c)
	c.c = make(chan struct{})
	c.cl.Unlock()
}
