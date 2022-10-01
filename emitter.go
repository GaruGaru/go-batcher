package batcher

import "time"

type BatchStats struct {
	size int
}

type EmitRule interface {
	Check(BatchStats) bool
}

type MultiEmitRule struct {
	Rules []EmitRule
}

func (m MultiEmitRule) Check(stats BatchStats) bool {
	for _, rule := range m.Rules {
		if rule.Check(stats) {
			return true
		}
	}
	return false
}

type SizeBasedEmit struct {
	targetSize int
}

func OnSizeReached(targetSize int) *SizeBasedEmit {
	return &SizeBasedEmit{targetSize: targetSize}
}

func (s *SizeBasedEmit) Check(stats BatchStats) bool {
	return stats.size > s.targetSize
}

type TimeBasedEmit struct {
	emitTimeout  time.Duration
	lastEmission time.Time
}

func Every(emitTimeout time.Duration) *TimeBasedEmit {
	return &TimeBasedEmit{emitTimeout: emitTimeout, lastEmission: time.Now()}
}

func (s *TimeBasedEmit) Check(_ BatchStats) bool {
	if time.Since(s.lastEmission) > s.emitTimeout {
		s.lastEmission = time.Now()
		return true
	}
	return false
}
