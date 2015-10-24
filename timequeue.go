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
	heap.Push(&q.messageHeap, message)
	defer q.afterHeapUpdate()
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

func (q *TimeQueue) Pop(release bool) *Message {
	defer q.afterHeapUpdate()
	q.messageLock.Lock()
	defer q.messageLock.Unlock()
	if len(q.messageHeap) == 0 {
		return nil
	}
	message := heap.Pop(&q.messageHeap).(*Message)
	if release {
		q.release(message)
	}
	return message
}

func (q *TimeQueue) PopAll(release bool) []*Message {
	defer q.afterHeapUpdate()
	q.messageLock.Lock()
	defer q.messageLock.Unlock()
	result := make([]*Message, 0, q.messageHeap.Len())
	for q.messageHeap.Len() > 0 {
		message := heap.Pop(&q.messageHeap).(*Message)
		result = append(result, message)
	}
	q.release(result...)
	return result
}

func (q *TimeQueue) PopUntil(until time.Time) []*Message {
	defer q.afterHeapUpdate()
	q.messageLock.Lock()
	defer q.messageLock.Unlock()
	result := make([]*Message, 0, q.messageHeap.Len())
	for q.messageHeap.Len() > 0 {
		message := heap.Pop(&q.messageHeap).(*Message)
		if message.Time.Sub(until) >= 0 {
			break
		}
		result = append(result, message)
	}
	q.release(result...)
	return result
}

func (q *TimeQueue) afterHeapUpdate() {
	if q.IsRunning() {
		q.updateAndSpawnWakeSignal()
	}
}

func (q *TimeQueue) Messages() <-chan *Message {
	return q.messageChan
}

func (q *TimeQueue) Size() int {
	q.messageLock.RLock()
	defer q.messageLock.RUnlock()
	return q.messageHeap.Len()
}

func (q *TimeQueue) Start() {
	if q.IsRunning() {
		return
	}
	q.setRunning(true)
	go q.run()
	q.updateAndSpawnWakeSignal()
}

func (q *TimeQueue) IsRunning() bool {
	q.stateLock.RLock()
	defer q.stateLock.RUnlock()
	return q.running
}

func (q *TimeQueue) run() {
runLoop:
	for {
		select {
		case <-q.wakeChan:
			q.onWake()
		case <-q.stopChan:
			break runLoop
		}
	}
}

func (q *TimeQueue) onWake() {
	q.releaseSingleMessage()
	q.setWakeSignal(nil)
	q.updateAndSpawnWakeSignal()
}

func (q *TimeQueue) releaseSingleMessage() {
	q.messageLock.Lock()
	defer q.messageLock.Unlock()
	if len(q.messageHeap) == 0 {
		return
	}
	message := heap.Pop(&q.messageHeap).(*Message)
	q.release(message)
}

func (q *TimeQueue) release(messages ...*Message) {
	go func() {
		for _, message := range messages {
			q.messageChan <- message
		}
	}()
}

func (q *TimeQueue) updateAndSpawnWakeSignal() {
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
