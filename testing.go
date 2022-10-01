package batcher

import "sync"

type testableProcessFn[I any] struct {
	l       *sync.Mutex
	Calls   [][]I
	mockFns []func([]I) error
}

func newTestableProcessFn[I any](fns ...func([]I) error) *testableProcessFn[I] {
	return &testableProcessFn[I]{
		mockFns: fns,
		l:       &sync.Mutex{},
	}
}

func (t *testableProcessFn[I]) process(batch []I) error {
	t.l.Lock()
	t.Calls = append(t.Calls, batch)
	for _, fn := range t.mockFns {
		if err := fn(batch); err != nil {
			return err
		}
	}
	t.l.Unlock()
	return nil
}
