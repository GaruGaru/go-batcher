package batcher

type options[I any] struct {
	maxSize   int
	workers   int
	processFn func([]I) error
	errorFn   func(error)
	emitRule  EmitRule
}

type Opt[I any] interface {
	Apply(*options[I])
}

func Emit[I any](rules ...EmitRule) EmitRuleOpt[I] {
	return EmitRuleOpt[I]{
		rule: MultiEmitRule{Rules: rules},
	}
}

func Workers[I any](workers int) WorkersOpt[I] {
	return WorkersOpt[I]{workers: workers}
}

func MaxSize[I any](maxSize int) MaxSizeOpt[I] {
	return MaxSizeOpt[I]{maxSize: maxSize}
}

func Process[I any](fn func([]I) error) ProcessFnOpt[I] {
	return ProcessFnOpt[I]{fn: fn}
}

func Error[I any](fn func(error)) ErrProcessFnOpt[I] {
	return ErrProcessFnOpt[I]{fn: fn}
}

type WorkersOpt[I any] struct {
	workers int
}

func (m WorkersOpt[I]) Apply(o *options[I]) {
	o.workers = m.workers
}

type MaxSizeOpt[I any] struct {
	maxSize int
}

func (m MaxSizeOpt[I]) Apply(o *options[I]) {
	o.maxSize = m.maxSize
}

type ProcessFnOpt[I any] struct {
	fn func([]I) error
}

func (m ProcessFnOpt[I]) Apply(o *options[I]) {
	o.processFn = m.fn
}

type ErrProcessFnOpt[I any] struct {
	fn func(error)
}

func (m ErrProcessFnOpt[I]) Apply(o *options[I]) {
	o.errorFn = m.fn
}

type EmitRuleOpt[I any] struct {
	rule EmitRule
}

func (m EmitRuleOpt[I]) Apply(o *options[I]) {
	o.emitRule = m.rule
}
