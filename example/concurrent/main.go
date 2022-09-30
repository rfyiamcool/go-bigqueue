package main

import (
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/rfyiamcool/go-bigqueue"
)

func randString(n int) string {
	return strings.Repeat(".", n)
}

func main() {
	var (
		count       = 100000            // 10w
		bucketBytes = 100 * 1024 * 1024 // 100mb
		maxBuckets  = 10                // 100mb * 10
		wg          = sync.WaitGroup{}
	)

	queue := bigqueue.NewQueueChains(bucketBytes, maxBuckets)

	incr := 0
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < count; i++ {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Microsecond) // < 10us

			val, err := queue.Pop()
			for err == bigqueue.ErrEmptyQueue {
				time.Sleep(1 * time.Microsecond)
				val, err = queue.Pop()
			}

			str := string(val)
			if strings.HasSuffix(str, "}}") && strings.HasPrefix(str, "{{") {
				incr++
				continue
			}

			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < count; i++ {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Microsecond) // < 10us

			length := rand.Intn(1024)
			bs := "{{" + randString(length) + "}}"
			queue.Push([]byte(bs))
		}
	}()
	wg.Wait()

	if incr != count {
		log.Panicf("counter error")
	}

	log.Println("ok")
}
