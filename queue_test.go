package bigqueue

import (
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestBigQueueChain1(t *testing.T) {
	payload := []byte("aabbccdd")
	qc := NewQueueChains(10, 2)

	// ok
	err := qc.Push(payload)
	assert.Equal(t, err, nil)
	assert.Equal(t, qc.BucketLength(), 1)

	// ok
	err = qc.Push(payload)
	assert.Equal(t, err, nil)
	assert.Equal(t, qc.BucketLength(), 2)

	// fail
	err = qc.Push(payload)
	assert.Equal(t, err, ErrOverflowQueue)

	// ok
	data, err := qc.Pop()
	assert.Equal(t, err, nil)
	assert.Equal(t, data, payload)
	assert.Equal(t, qc.BucketLength(), 2)

	// ok
	qc.Pop()
	assert.Equal(t, qc.chains.Len(), 1)

	// fail
	bs, err := qc.Pop()
	assert.Equal(t, len(bs), 0)
	assert.Equal(t, qc.chains.Len(), 1)
	assert.Equal(t, err, ErrEmptyQueue)

	for i := 0; i < 5; i++ {
		err := qc.Push([]byte(cast.ToString(i)))
		assert.Equal(t, err, nil)
	}

	for i := 0; i < 5; i++ {
		val, err := qc.Pop()
		assert.Equal(t, err, nil)
		assert.Equal(t, val, []byte(cast.ToString(i)))
	}
}

func TestBigQueueChain2(t *testing.T) {
	qc := NewQueueChains(1*1024*1024, 10)
	count := 10000

	for i := 0; i < count; i++ {
		length := rand.Intn(1024)
		bs := "{{" + randString(length) + "}}"
		qc.Push([]byte(bs))
	}

	incr := 0
	for i := 0; i < count; i++ {
		val, err := qc.Pop()
		if err != nil {
			continue
		}

		str := string(val)
		if strings.HasSuffix(str, "}}") && strings.HasPrefix(str, "{{") {
			incr++
			continue
		}

		t.Fatal("bad")
	}

	assert.Equal(t, incr, count)
	t.Log(incr)
}

func TestBigQueueChain3(t *testing.T) {
	qc := NewQueueChains(10*1024*1024, 10) // 10mb * 10 = 100mb
	count := 100000                        // 10w
	wg := sync.WaitGroup{}

	incr := 0
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < count; i++ {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Microsecond) // < 10us

			val, err := qc.Pop()
			for err == ErrEmptyQueue {
				time.Sleep(1 * time.Microsecond)
				val, err = qc.Pop()
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
			qc.Push([]byte(bs))
		}
	}()

	wg.Wait()
	assert.Equal(t, incr, count)
	t.Log(incr)
}

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
