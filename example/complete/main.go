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
		batcher.Workers[string](3),
		batcher.MaxSize[string](10),
		batcher.Emit[string](
			batcher.OnSizeReached(10),
			batcher.Every(100*time.Millisecond),
		),
		batcher.Process(func(items []string) error {
			fmt.Println(strings.Join(items, ", "))
			return nil
		}),
		batcher.Error[string](func(err error) {
			fmt.Println(err.Error())
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
