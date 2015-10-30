package timequeue

import (
	"sync"
	"time"
)

const (
	DefaultSize = 1
)

type TimeQueue struct {
	lock *sync.Mutex

	messages *messageHeap

	running    bool
	wakeSignal *wakeSignal

	messageChan chan *Message
	stopChan    chan struct{}
	wakeChan    chan time.Time
}

func New() *TimeQueue {
	return NewCapacity(DefaultSize)
}

func NewCapacity(capacity int) *TimeQueue {
	return &TimeQueue{
		lock:        &sync.Mutex{},
		messages:    newMessageHeap(),
		running:     false,
		wakeSignal:  nil,
		messageChan: make(chan *Message, capacity),
		stopChan:    make(chan struct{}),
		wakeChan:    make(chan time.Time),
	}
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
	q.lock.Lock()
	defer q.lock.Unlock()
	q.messages.pushMessage(message)
	q.afterHeapUpdate()
}

func (q *TimeQueue) Peek() (time.Time, interface{}) {
	message := q.PeekMessage()
	if message == nil {
		return time.Time{}, nil
	}
	return message.Time, message.Data
}

func (q *TimeQueue) PeekMessage() *Message {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.peekMessage()
}

func (q *TimeQueue) peekMessage() *Message {
	return q.messages.peekMessage()
}

func (q *TimeQueue) Pop(release bool) *Message {
	q.lock.Lock()
	defer q.lock.Unlock()
	message := q.messages.popMessage()
	if message == nil {
		return nil
	}
	if release {
		q.releaseMessage(message)
	}
	q.afterHeapUpdate()
	return message
}

func (q *TimeQueue) PopAll(release bool) []*Message {
	q.lock.Lock()
	defer q.lock.Unlock()
	result := make([]*Message, 0, q.messages.Len())
	for message := q.messages.popMessage(); message != nil; message = q.messages.popMessage() {
		result = append(result, message)
	}
	if release {
		q.releaseCopyToChan(result)
	}
	q.afterHeapUpdate()
	return result
}

func (q *TimeQueue) PopAllUntil(until time.Time, release bool) []*Message {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.popAllUntil(until, release)
}

func (q *TimeQueue) popAllUntil(until time.Time, release bool) []*Message {
	result := make([]*Message, 0, q.messages.Len())
	for message := q.messages.peekMessage(); message != nil && message.Before(until); message = q.messages.peekMessage() {
		result = append(result, q.messages.popMessage())
	}
	if release {
		q.releaseCopyToChan(result)
	}
	q.afterHeapUpdate()
	return result
}

func (q *TimeQueue) afterHeapUpdate() {
	if q.isRunning() {
		q.updateAndSpawnWakeSignal()
	}
}

func (q *TimeQueue) Messages() <-chan *Message {
	return q.messageChan
}

func (q *TimeQueue) Size() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.messages.Len()
}

func (q *TimeQueue) Start() {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.isRunning() {
		return
	}
	q.setRunning(true)
	go q.run()
	q.updateAndSpawnWakeSignal()
}

func (q *TimeQueue) IsRunning() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.isRunning()
}

func (q *TimeQueue) isRunning() bool {
	return q.running
}

func (q *TimeQueue) run() {
	//runLoop:
	for {
		select {
		case wakeTime := <-q.wakeChan:
			q.onWake(wakeTime)
		case <-q.stopChan:
			return
			//break runLoop
		}
	}
}

func (q *TimeQueue) onWake(wakeTime time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.popAllUntil(wakeTime, true)
	q.updateAndSpawnWakeSignal()
}

func (q *TimeQueue) releaseMessage(message *Message) {
	go func() {
		q.messageChan <- message
	}()
}

func (q *TimeQueue) releaseCopyToChan(messages []*Message) {
	copyChan := make(chan *Message, len(messages))
	for _, message := range messages {
		copyChan <- message
	}
	q.releaseChan(copyChan)
	close(copyChan)
}

func (q *TimeQueue) releaseChan(messages <-chan *Message) {
	go func() {
		for message := range messages {
			q.messageChan <- message
		}
	}()
}

func (q *TimeQueue) updateAndSpawnWakeSignal() bool {
	q.killWakeSignal()
	message := q.peekMessage()
	if message == nil {
		return false
	}
	q.setWakeSignal(newWakeSignal(q.wakeChan, message.Time))
	return q.spawnWakeSignal()
}

func (q *TimeQueue) setWakeSignal(wakeSignal *wakeSignal) {
	q.wakeSignal = wakeSignal
}

func (q *TimeQueue) spawnWakeSignal() bool {
	if q.wakeSignal != nil {
		q.wakeSignal.spawn()
		return true
	}
	return false
}

func (q *TimeQueue) killWakeSignal() bool {
	if q.wakeSignal != nil {
		q.wakeSignal.kill()
		q.wakeSignal = nil
		return true
	}
	return false
}

func (q *TimeQueue) Stop() {
	q.lock.Lock()
	defer q.lock.Unlock()
	if !q.isRunning() {
		return
	}
	q.killWakeSignal()
	q.setRunning(false)
	go func() {
		q.stopChan <- struct{}{}
	}()
}

func (q *TimeQueue) setRunning(running bool) {
	q.running = running
}

type wakeSignal struct {
	dst  chan time.Time
	src  <-chan time.Time
	stop chan struct{}
}

func newWakeSignal(dst chan time.Time, wakeTime time.Time) *wakeSignal {
	return &wakeSignal{
		dst:  dst,
		src:  time.After(wakeTime.Sub(time.Now())),
		stop: make(chan struct{}),
	}
}

func (w *wakeSignal) spawn() {
	go func() {
		select {
		case wakeTime := <-w.src:
			w.dst <- wakeTime
		case <-w.stop:
		}
		w.src = nil
	}()
}

func (w *wakeSignal) kill() {
	close(w.stop)
}
