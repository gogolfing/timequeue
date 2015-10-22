package timequeue

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultSize = 1
)

type TimeQueue struct {
	*sync.RWMutex
	messageHeap
	running  bool
	stop     chan struct{}
	nextTime time.Time
	nextDone chan struct{}

	messageChan chan *Message
	wakeChan    chan struct{}
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
		wakeChan:    make(chan struct{}),
	}
	heap.Init(&q.messageHeap)
	return q
}

func (q *TimeQueue) Push(time time.Time, data []byte) {
	q.PushMessage(&Message{
		Time: time,
		Data: data,
	})
}

func (q *TimeQueue) PushMessage(message *Message) {
	q.enqueue(message)
}

func (q *TimeQueue) Peek() (time.Time, []byte) {
	message := q.PeekMessage()
	return message.Time, message.Data
}

func (q *TimeQueue) PeekMessage() *Message {
	q.RLock()
	defer q.RUnlock()
	message := q.messageHeap[0]
	return message
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
	<-q.stop
}

func (q *TimeQueue) wake() {
	go func() {
		q.wakeChan <- struct{}{}
	}()
}

func (q *TimeQueue) run() {
	for {
		select {
		case <-q.wakeChan:
			q.dequeue()
		case <-q.stop:
			q.stop <- struct{}{}
			break
		}
	}
}

func (q *TimeQueue) enqueue(message *Message) {
	q.Lock()
	defer q.Unlock()
	if len(q.messageHeap) > 0 {
		nextTime := q.messageHeap[0].Time
		if message.Time.Sub(nextTime) < 0 {
			q.nextDone <- struct{}{}
			<-q.nextDone
		}
	}
	heap.Push(&q.messageHeap, message)
	nextTime := q.messageHeap[0]
	nextTimeChan := time.After(nextTime.Sub(time.Now()))
	go sendSignal(q.wakeChan, nextTimeChan, q.nextDone)
}

func (q *TimeQueue) dequeue() {
	q.Lock()
	defer q.Unlock()
	if len(q.messageHeap) == 0 {
		return
	}
	message := heap.Pop(&q.messageHeap).(*Message)
	fmt.Println(message)
	go q.deploy(message)
}

func (q *TimeQueue) deploy(message *Message) {
	q.messageChan <- message
}

func sendSignal(dst chan struct{}, src <-chan time.Time, done chan struct{}) {
	select {
	case <-src:
		dst <- struct{}{}
	case <-done:
		done <- struct{}{}
	}
}
