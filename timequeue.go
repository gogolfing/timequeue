package timequeue

import (
	"sync"
	"time"
)

const (
	//The default capacity used for Messages() channels.
	DefaultCapacity = 1
)

//Type TimeQueue is a queue of Messages that releases its Messages when their
//Time fields pass.
//
//When Messages are pushed to a TimeQueue, the earliest Message is used to
//determine when the TimeQueue should wake. Upon waking, that earliest Message
//is "released" from the TimeQueue by being sent on the channel returned by
//Messages().
//
//Messages may be pushed an popped from a TimeQueue whether or not the TimeQueue
//is running or not. Start() and Stop() may be called as many times as desired,
//but Messsages will be released only between calls to Start() and Stop(), i.e.
//while the TimeQueue is running and IsRunning() returns true.
//
//Calls to Pop(), PopAll(), and PopAllUntil() may be called to remove Messages
//from a TimeQueue, but this is required for normal use.
//
//One of the New*() functions should be used to create a TimeQueue. A zero-value
//TimeQueue is not in a valid or working state.
type TimeQueue struct {
	//the goal is to have only one go-routine "inside" a TimeQueue at once.
	//this is acheived by locking on lock in all exported methods and
	//requiring the TimeQueue be locked in all unexported methods and
	//before all use of unexported fields.

	//protects all other members of a TimeQueue.
	lock *sync.Mutex

	//the heap of Messages in the TimeQueue.
	messages *messageHeap

	//flag determining if the TimeQueue is running.
	//should be true between calls to Start() and Stop() and false otherwise.
	running bool
	//signal that sends to stopChan or wakeChan to wake or stop the running go-routine.
	wakeSignal *wakeSignal

	//the channel to send released Messages on. should be receive only in client code.
	messageChan chan *Message
	//send to this channel to wake the running go-routine and release Messages.
	wakeChan chan time.Time
	//send to this channel to stop the running go-routine.
	stopChan chan struct{}
}

//New creates a new *TimeQueue with a call to New(DefaultCapacity).
func New() *TimeQueue {
	return NewCapacity(DefaultCapacity)
}

//NewCapacity creates a new *TimeQueue where the channel returned from Messages()
//has the capacity given by capacity.
//The new TimeQueue is in the stopped state and has no Messages in it.
func NewCapacity(capacity int) *TimeQueue {
	return &TimeQueue{
		lock:        &sync.Mutex{},
		messages:    newMessageHeap(),
		running:     false,
		wakeSignal:  nil,
		messageChan: make(chan *Message, capacity),
		wakeChan:    make(chan time.Time),
		stopChan:    make(chan struct{}),
	}
}

//Push creates and adds a Message to q with time and data. The created Message is returned.
func (q *TimeQueue) Push(time time.Time, data interface{}) *Message {
	message := &Message{
		Time: time,
		Data: data,
	}
	q.PushMessage(message)
	return message
}

//PushMessage adds message to q.
func (q *TimeQueue) PushMessage(message *Message) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.messages.pushMessage(message)
	q.afterHeapUpdate()
}

//Peek returns (without removing) the Time and Data fields from the earliest
//Message in q.
//If q is empty, then the zero Time and nil are returned.
func (q *TimeQueue) Peek() (time.Time, interface{}) {
	message := q.PeekMessage()
	if message == nil {
		return time.Time{}, nil
	}
	return message.Time, message.Data
}

//PeekMessage returns (without removing) the earliest Message in q or nil if q
//is empty.
func (q *TimeQueue) PeekMessage() *Message {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.peekMessage()
}

//peekMessage is the unexported version of PeekMessage().
//It should only be called when q is locked.
func (q *TimeQueue) peekMessage() *Message {
	return q.messages.peekMessage()
}

//Pop removes and returns the earliest Message in q or nil if q is empty.
//If release is true, then the Message (if not nil) will also be sent on the
//channel returned from Messages().
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

//PopAll removes and returns a slice of all Messages in q.
//The returned slice will be non-nil but empty if q is itseld empty.
//If release is true, then all returned Messages will also be sent on the channel
//returned from Messages().
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

//PopAllUntil removes and returns a slice of Messages in q with Time fields before,
//but not equal to, until.
//If release is true, then all returned Messages will also be sent on the channel
//returned from Messages().
func (q *TimeQueue) PopAllUntil(until time.Time, release bool) []*Message {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.popAllUntil(until, release)
}

//popAllUntil is the unexported verson of PopAllUntil.
//It should only be called when q is locked.
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

//afterHeapUpdate ensures the earliest time is in the next wake signal, if q is running.
//It should only be called when q is locked.
func (q *TimeQueue) afterHeapUpdate() {
	if q.isRunning() {
		q.updateAndSpawnWakeSignal()
	}
}

//Messages returns the receive only channel that all Messages are released on.
//The returned channel will be the same instance on every call, and this value
//will never be closed.
//
//In order to receive Messages when they are earliest available a go-routine should
//be spawned to drain the channel of all Messages.
//	q := timequeue.New()
//	q.Start()
//	go func() {
//		message := <-q.Messages()
//	}()
//	//push Messages to q.
func (q *TimeQueue) Messages() <-chan *Message {
	return q.messageChan
}

//Size returns the number of Messages in q. This is the number of Messages that
//have yet to be released (or waiting to be sent on Messages()) in q.
//Therefore, there could still be Messages that q has reference to that are waiting
//to be released or in the Messages() channel buffer.
//
//To obtain the number of total Messages that q still has refernce to add this value
//and the length of Messages():
//	q.Size() + len(q.Messages())
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
	for {
		select {
		case wakeTime := <-q.wakeChan:
			q.onWake(wakeTime)
		case <-q.stopChan:
			return
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
