package batcher

import (
	"context"
	"time"
)

type BatchStats struct {
	size int
}

type EmitRule interface {
	Check(BatchStats)
	Init(chan struct{})
	Emit() chan struct{}
	Close()
}

type MultiEmitRule struct {
	Rules  []EmitRule
	emitCh chan struct{}
}

func NewMultiEmitRule(rules []EmitRule) *MultiEmitRule {
	emitCh := make(chan struct{})
	for _, r := range rules {
		r.Init(emitCh)
	}
	return &MultiEmitRule{Rules: rules, emitCh: emitCh}
}

func (m *MultiEmitRule) Emit() chan struct{} {
	return m.emitCh
}

func (m *MultiEmitRule) Check(stats BatchStats) {
	for _, rule := range m.Rules {
		rule.Check(stats)
	}
}

func (m *MultiEmitRule) Init(_ chan struct{}) {
}

func (m *MultiEmitRule) Close() {
	for _, r := range m.Rules {
		r.Close()
	}
	close(m.emitCh)
}

type SizeBasedEmit struct {
	targetSize int
	emitCh     chan struct{}
}

func OnSizeReached(targetSize int) *SizeBasedEmit {
	return &SizeBasedEmit{targetSize: targetSize}
}

func (s *SizeBasedEmit) Check(stats BatchStats) {
	if stats.size > s.targetSize {
		select {
		case s.emitCh <- struct{}{}:
		default:
		}
	}
}

func (s *SizeBasedEmit) Init(ec chan struct{}) {
	s.emitCh = ec
}

func (s *SizeBasedEmit) Emit() chan struct{} {
	return s.emitCh
}

func (s *SizeBasedEmit) Close() {
}

type TimeBasedEmit struct {
	emitTimeout time.Duration
	emitCh      chan struct{}
	ticker      *time.Ticker
	cancel      func()
}

func Every(emitTimeout time.Duration) *TimeBasedEmit {
	return &TimeBasedEmit{emitTimeout: emitTimeout}
}

func (s *TimeBasedEmit) Check(_ BatchStats) {
}

func (s *TimeBasedEmit) Init(ec chan struct{}) {
	s.emitCh = ec
	s.ticker = time.NewTicker(s.emitTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-s.ticker.C:
			s.emitCh <- struct{}{}
		case <-ctx.Done():
			return
		}
	}()
	s.cancel = cancel
}

func (s *TimeBasedEmit) Emit() chan struct{} {
	return s.emitCh
}

func (s *TimeBasedEmit) Close() {
	s.ticker.Stop()
	s.cancel()
}
