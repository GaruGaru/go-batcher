# go-batcher  [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

High performance, easy to use batcher written in Go.

## Installation

`go get -u github.com/garugaru/go-batcher`

## Quick Start

```go
batch := batcher.NewBatcher[string](
	batcher.Emit[string](
		batcher.Every(100*time.Millisecond),
	),
	batcher.Process(func(items []string) error {
		fmt.Println(strings.Join(items, ","))
		return nil
	}),
)

batch.Start(context.Background())

batch.Accumulate("hello", "world", "!") 

if err := batch.Wait(); err != nil {
	panic(err)
}
```

[ci-img]: https://github.com/garugaru/go-batcher/actions/workflows/tests.yml/badge.svg
[cov-img]: https://codecov.io/gh/garugaru/go-batcher/branch/master/graph/badge.svg
[ci]: https://github.com/garugaru/go-batcher/actions/workflows/tests.yml
[cov]: https://codecov.io/gh/garugaru/go-batcher