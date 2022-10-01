package main

import (
	"context"
	"fmt"
	"github.com/garugaru/go-batcher"
	"strconv"
	"strings"
	"time"
)

func main() {
	batch := batcher.NewBatcher[string](
		batcher.Emit[string](
			batcher.Every(100*time.Millisecond),
		),
		batcher.Process(func(items []string) error {
			fmt.Println(strings.Join(items, ", "))
			return nil
		}),
	)

	batch.Start(context.Background())

	go func() {
		<-time.NewTimer(5 * time.Second).C
		batch.Terminate()
	}()

	for i := 0; i < 100; i++ {
		batch.Accumulate(strconv.Itoa(i))
	}

	if err := batch.Wait(); err != nil {
		panic(err)
	}
}
