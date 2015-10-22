package timequeue

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultSize = 128
)

type TimeQueue struct {
	*sync.RWMutex
	messageHeap
	running bool
	stop    chan struct{}

	messageChan chan *Message
}

func New() *TimeQueue {
	return NewSize(DefaultSize)
}

func NewSize(size int) *TimeQueue {
	q := &TimeQueue{
		RWMutex:     &sync.RWMutex{},
		messageHeap: messageHeap([]*Message{}),
		running:     false,
		stop:        nil,
		messageChan: make(chan *Message, size),
	}
	heap.Init(&q.messageHeap)
	return q
}

func (q *TimeQueue) Push(time time.Time, data []byte) {
}

func (q *TimeQueue) PushMessage(time time.Time, message *Message) {
}

func (q *TimeQueue) Peek() (time.Time, []byte) {
	return time.Now(), nil
}

func (q *TimeQueue) PeekMessage() *Message {
	return nil
}

func (q *TimeQueue) Messages() <-chan *Message {
	return q.messageChan
}

func (q *TimeQueue) Start() {
	q.Lock()
	defer q.Unlock()
	if q.running {
		return
	}
	q.stop = make(chan struct{})
	q.running = true
	go q.run()
}

func (q *TimeQueue) Stop() {
	q.Lock()
	defer q.Unlock()
	if !q.running || q.stop == nil {
		return
	}
	q.running = false
	go func() {
		q.stop <- struct{}{}
	}()
}

func (q *TimeQueue) run() {
	for {
		sleep := time.Duration(1) * time.Second
		fmt.Println("awake")
		time.Sleep(sleep)
	}
}
