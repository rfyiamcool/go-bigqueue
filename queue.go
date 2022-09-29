package bigqueue

import (
	"container/list"
	"sync"
	"sync/atomic"
)

const oneMB float32 = 1 * 1024 * 1024
const oneGB float32 = 1 * 1024 * 1024 * 1024

func MB(n float32) int {
	return int(n * oneMB) // nolint
}

func GB(n float32) int {
	return int(n * oneGB) // nolint
}

type BigQueue struct {
	sync.Mutex
	queue *BytesQueue // code from bigcache/queue
	chain *list.List
}

func NewQueue(capacity int, maxCapacity int) *BigQueue {
	return &BigQueue{
		queue: NewBytesQueue(capacity, maxCapacity, false),
		chain: list.New(),
	}
}

func (bq *BigQueue) Reset() {
	bq.Lock()
	defer bq.Unlock()

	bq.queue.Reset()
}

func (bq *BigQueue) Push(data []byte) error {
	bq.Lock()
	defer bq.Unlock()

	_, err := bq.queue.Push(data)
	return err
}

func (bq *BigQueue) Pop() ([]byte, error) {
	bq.Lock()
	defer bq.Unlock()

	data, err := bq.queue.Pop()
	if err != nil {
		return nil, err
	}

	dst := make([]byte, len(data))
	copy(dst, data)
	return dst, nil
}

func (bq *BigQueue) Len() int {
	bq.Lock()
	defer bq.Unlock()

	return bq.queue.Len()
}

func (bq *BigQueue) Full() bool {
	bq.Lock()
	defer bq.Unlock()

	return bq.queue.full
}

type BigQueueChains struct {
	sync.Mutex

	bucketBytes int
	maxBuckets  int

	chains  *list.List // fifo chains, left side push, right side pop
	counter atomicInt64
}

// NewQueueChains the fifo queue is the same as redis quicklist (chains + ziplist)
func NewQueueChains(bucketBytes int, maxBuckets int) *BigQueueChains {
	chains := list.New()
	chains.PushBack(NewBytesQueue(bucketBytes, maxBuckets, false))

	return &BigQueueChains{
		chains:      chains,
		bucketBytes: bucketBytes,
		maxBuckets:  maxBuckets,
	}
}

func (bq *BigQueueChains) allocBucket() *BytesQueue {
	return NewBytesQueue(bq.bucketBytes, bq.bucketBytes, false)
}

func (bq *BigQueueChains) Reset() {
	for q := bq.chains.Front(); q != nil; q = q.Next() {
		q.Value.(*BigQueue).Reset()
	}
}

func (bq *BigQueueChains) Push(data []byte) error {
	bq.Lock()
	defer bq.Unlock()

	var queue *BytesQueue
	if bq.chains.Len() == 0 {
		queue = bq.allocBucket()
	} else {
		queue = bq.chains.Back().Value.(*BytesQueue)
	}

	// first times
	_, err := queue.Push(data)
	if err == nil { // ok
		bq.incr()
		return nil
	}
	if err != ErrOverflowQueue { // happen other exception error
		return err
	}
	if bq.chains.Len() >= bq.maxBuckets { // more than max buckets
		return err
	}

	// second times
	queue = bq.allocBucket()
	_, err = queue.Push(data)
	if err != nil {
		return err
	}

	bq.chains.PushBack(queue)
	bq.incr()
	return nil
}

func (bq *BigQueueChains) incr() {
	bq.counter.Add(1)
}

func (bq *BigQueueChains) decr() {
	bq.counter.Add(-1)
}

func (bq *BigQueueChains) Pop() ([]byte, error) {
	bq.Lock()
	defer bq.Unlock()

	if bq.chains.Len() == 0 {
		return nil, ErrEmptyQueue
	}

	queue := bq.chains.Front().Value.(*BytesQueue)
	if queue.Len() == 0 && bq.chains.Len() > 1 {
		bq.chains.Remove(bq.chains.Front())           // del first node
		queue = bq.chains.Front().Value.(*BytesQueue) // use current first node
	}

	data, err := queue.Pop()
	if err != nil {
		return nil, err
	}

	bq.decr()

	// copy
	dst := make([]byte, len(data))
	copy(dst, data)
	return dst, err
}

func (bq *BigQueueChains) Len() int64 {
	return bq.counter.Get()
}

func (bq *BigQueueChains) BucketLength() int {
	bq.Lock()
	defer bq.Unlock()

	return bq.chains.Len()
}

type atomicInt64 struct {
	int64
}

func newAtomicInt64(n int64) atomicInt64 {
	return atomicInt64{n}
}

func (i *atomicInt64) Add(n int64) int64 {
	return atomic.AddInt64(&i.int64, n)
}

func (i *atomicInt64) Set(n int64) {
	atomic.StoreInt64(&i.int64, n)
}

func (i *atomicInt64) Get() int64 {
	return atomic.LoadInt64(&i.int64)
}
