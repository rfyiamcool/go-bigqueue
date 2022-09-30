package main

import (
	"context"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rfyiamcool/go-bigqueue"
)

var (
	empty = struct{}{}
)

func randString(n int) string {
	return strings.Repeat(".", n)
}

func main() {
	var (
		ctx, cancel = context.WithCancel(context.TODO())

		bucketBytes       = 100 * 1024 * 1024 // 100mb
		maxBuckets        = 10                // 100mb * 10 = 1gb
		noticeChan        = make(chan struct{}, 20000)
		count       int64 = 2000000 // 200w
		workerNum         = 20

		start = time.Now()
		wg    = sync.WaitGroup{}
	)

	defer cancel()

	queue := bigqueue.NewQueueChains(bucketBytes, maxBuckets)

	// start consumers
	var readCounter int64
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case <-noticeChan:
				}

				val, err := queue.Pop()
				for err == bigqueue.ErrEmptyQueue {
					continue
				}

				str := string(val)
				if strings.HasSuffix(str, "}}") && strings.HasPrefix(str, "{{") {
					atomic.AddInt64(&readCounter, 1)
					continue
				}
			}
		}()
	}

	// start producers
	var incr int64
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				val := atomic.AddInt64(&incr, 1)
				if val > count {
					break
				}

				length := rand.Intn(1024)
				bs := "{{" + randString(length) + "}}"
				queue.Push([]byte(bs))

				select {
				case noticeChan <- empty:
				}
			}
		}()
	}

	for {
		if atomic.LoadInt64(&readCounter) == count {
			cancel()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	wg.Wait()
	if readCounter != count {
		log.Panicf("counter error")
	}

	log.Printf("start %v producers, start consumers %v \n", workerNum, workerNum)
	log.Printf("push %v msgs, consume %v msgs, cost: %v \n", count, readCounter, time.Since(start))
}
