package main

import (
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/rfyiamcool/go-bigqueue"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func randString(n int) string {
	return strings.Repeat(".", n)
}

func main() {
	var (
		count       = 100000            // 10w
		bucketBytes = 100 * 1024 * 1024 // 100mb
		maxBuckets  = 10                // 100mb * 10
	)

	queue := bigqueue.NewQueueChains(bucketBytes, maxBuckets)

	for i := 0; i < count; i++ {
		length := rand.Intn(1024)
		bs := "{{" + randString(length) + "}}"
		queue.Push([]byte(bs))
	}

	incr := 0
	for i := 0; i < count; i++ {
		val, err := queue.Pop()
		if err != nil {
			break
		}

		str := string(val)
		if strings.HasSuffix(str, "}}") && strings.HasPrefix(str, "{{") {
			incr++
			continue
		}

		panic("error")
	}

	if incr != count {
		log.Panicf("counter error")
	}

	log.Println("ok")
}
