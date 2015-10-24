package timequeue

import (
	"container/heap"
	"sync"
	"time"
)

const (
	DefaultSize = 1
)

type TimeQueue struct {
	messageLock *sync.RWMutex
	messageHeap messageHeap

	stateLock  *sync.RWMutex
	running    bool
	wakeSignal *wakeSignal

	messageChan chan *Message
	stopChan    chan struct{}
	wakeChan    chan struct{}
}

func New() *TimeQueue {
	return NewSize(DefaultSize)
}

func NewSize(size int) *TimeQueue {
	q := &TimeQueue{
		messageLock: &sync.RWMutex{},
		messageHeap: messageHeap([]*Message{}),
		stateLock:   &sync.RWMutex{},
		running:     false,
		wakeSignal:  nil,
		messageChan: make(chan *Message, size),
		stopChan:    make(chan struct{}),
		wakeChan:    make(chan struct{}),
	}
	heap.Init(&q.messageHeap)
	return q
}

func (q *TimeQueue) Push(time time.Time, data interface{}) *Message {
	message := &Message{
		Time: time,
		Data: data,
	}
	q.PushMessage(message)
	return message
}

func (q *TimeQueue) PushMessage(message *Message) {
	if message == nil {
		return
	}
	spawnNew := q.shouldSpanWakeSignal(message)
	heap.Push(&q.messageHeap, message)
	if spawnNew {
		q.createAndSpawnWakeSignal()
	}
}

func (q *TimeQueue) shouldSpanWakeSignal(message *Message) bool {
	old := q.PeekMessage()
	if old == nil {
		return true
	}
	if message.Time.Sub(old.Time) < 0 {
		return true
	}
	return false
}

func (q *TimeQueue) Peek() (time.Time, interface{}) {
	message := q.PeekMessage()
	if message == nil {
		return time.Time{}, nil
	}
	return message.Time, message.Data
}

func (q *TimeQueue) PeekMessage() *Message {
	q.messageLock.RLock()
	defer q.messageLock.RUnlock()
	if len(q.messageHeap) > 0 {
		return q.messageHeap[0]
	}
	return nil
}

func (q *TimeQueue) Pop() *Message {
	return nil
}

func (q *TimeQueue) Messages() <-chan *Message {
	return q.messageChan
}

func (q *TimeQueue) Start() {
	if q.IsRunning() {
		return
	}
	q.setRunning(true)
	go q.run()
	q.createAndSpawnWakeSignal()
}

func (q *TimeQueue) IsRunning() bool {
	q.stateLock.RLock()
	defer q.stateLock.RUnlock()
	return q.running
}

func (q *TimeQueue) run() {
	for {
		select {
		case <-q.wakeChan:
			q.onWake()
		case <-q.stopChan:
			break
		}
	}
}

func (q *TimeQueue) onWake() {
	q.releaseSingleMessage()
	q.setWakeSignal(nil)
	q.createAndSpawnWakeSignal()
}

func (q *TimeQueue) releaseSingleMessage() {
	q.messageLock.Lock()
	defer q.messageLock.Unlock()
	if len(q.messageHeap) == 0 {
		return
	}
	message := heap.Pop(&q.messageHeap).(*Message)
	go func() {
		q.messageChan <- message
	}()
}

func (q *TimeQueue) createAndSpawnWakeSignal() {
	q.killWakeSignal()
	message := q.PeekMessage()
	if message == nil {
		return
	}
	q.setWakeSignal(newWakeSignal(q.wakeChan, message.Time))
	q.spawnWakeSignal()
}

func (q *TimeQueue) setWakeSignal(wakeSignal *wakeSignal) {
	q.stateLock.Lock()
	defer q.stateLock.Unlock()
	q.wakeSignal = wakeSignal
}

func (q *TimeQueue) spawnWakeSignal() {
	q.stateLock.RLock()
	defer q.stateLock.RUnlock()
	if q.wakeSignal != nil {
		q.wakeSignal.spawn()
	}
}

func (q *TimeQueue) killWakeSignal() {
	q.stateLock.RLock()
	defer q.stateLock.RUnlock()
	if q.wakeSignal != nil {
		q.wakeSignal.kill()
	}
}

func (q *TimeQueue) Stop() {
	if !q.IsRunning() {
		return
	}
	q.killWakeSignal()
	q.setRunning(false)
	go func() {
		q.stopChan <- struct{}{}
	}()
}

func (q *TimeQueue) setRunning(running bool) {
	q.stateLock.Lock()
	defer q.stateLock.Unlock()
	q.running = running
}

type wakeSignal struct {
	dst  chan struct{}
	src  <-chan time.Time
	stop chan struct{}
}

func newWakeSignal(dst chan struct{}, wakeTime time.Time) *wakeSignal {
	return &wakeSignal{
		dst:  dst,
		src:  time.After(wakeTime.Sub(time.Now())),
		stop: make(chan struct{}),
	}
}

func (w *wakeSignal) spawn() {
	go func() {
		select {
		case <-w.src:
			w.dst <- struct{}{}
		case <-w.stop:
		}
		w.src = nil
	}()
}

func (w *wakeSignal) kill() {
	close(w.stop)
}
