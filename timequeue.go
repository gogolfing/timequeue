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
//Messages may be pushed and popped from a TimeQueue whether or not the TimeQueue
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

//Start spawns a new go-routine to listen for wake times of Messages and sets the
//state to running.
//If q is already running, then Start is a nop.
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

//IsRunning returns whether or not q is running. E.g. in between calls to Start()
//and Stop().
//If IsRunning returns true, then it is possible that Messages are being released.
func (q *TimeQueue) IsRunning() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.isRunning()
}

//isRunning is the unexported version of IsRunning.
//It should only be called when q is locked.
func (q *TimeQueue) isRunning() bool {
	return q.running
}

//run is the run loop of a TimeQueue.
//It is an infinite loop that selects between q.wakeChan and q.stopChan.
//If q.wakeChan is selected, then q.onWake() is called.
//If q.wakeStop is selected, then this method returns.
//Note that this method does not spawn a new go-routine.
//That should be done outside of run.
//run does not have the precondition that q must be locked.
//This is a function that should execute in its own go-routine and thus cannot
//lock any other parts of q.
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

//onWake should be called when q receives a value on q.wakeChan.
//Because onWake will be called from a go-routine that we spawned, we lock and
//defer unlock on q since this acts like an exported method of sorts in that
//it starts execution of unexported code from an outside go-routine.
func (q *TimeQueue) onWake(wakeTime time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.popAllUntil(wakeTime, true)
	q.updateAndSpawnWakeSignal()
}

//releaseMessage is a utility method that spawns a go-routine to send message on
//q.messageChan so that that calling go-routine does not have to wait.
func (q *TimeQueue) releaseMessage(message *Message) {
	go func() {
		q.messageChan <- message
	}()
}

//releaseCopyToChan is a utility method that copies messages to a new, buffered
//channel, and empties that new channel by sending every messsage on q.messageChan.
func (q *TimeQueue) releaseCopyToChan(messages []*Message) {
	copyChan := make(chan *Message, len(messages))
	for _, message := range messages {
		copyChan <- message
	}
	q.releaseChan(copyChan)
	close(copyChan)
}

//releaseChan is a utility method that spawns a go-routine to send every message
//in messages on q.messageChan.
//Note that releaseChan reads from messages until it is closed, thus messages must
//be closed by the calling function.
func (q *TimeQueue) releaseChan(messages <-chan *Message) {
	go func() {
		for message := range messages {
			q.messageChan <- message
		}
	}()
}

//updateAndSpawnSignal kills the current wake signal if it exists
//and creates and spawns the next wake signal if there are any messages left in q.
//Returns true if a new wakeSignal was spawned, false otherwise.
//It should only be called when q is locked.
func (q *TimeQueue) updateAndSpawnWakeSignal() bool {
	q.killWakeSignal()
	message := q.peekMessage()
	if message == nil {
		return false
	}
	q.setWakeSignal(newWakeSignal(q.wakeChan, message.Time))
	return q.spawnWakeSignal()
}

//setWakeSignal sets q.wakeSignal to wakeSignal.
//It should only be called when q is locked.
func (q *TimeQueue) setWakeSignal(wakeSignal *wakeSignal) {
	q.wakeSignal = wakeSignal
}

//spawnWakeSignal calls spawn() on q.wakeSignal if it is not nil.
//Returns true if spawn was called, false otherwise.
//It should only be called when q is locked.
func (q *TimeQueue) spawnWakeSignal() bool {
	if q.wakeSignal != nil {
		q.wakeSignal.spawn()
		return true
	}
	return false
}

//killWakeSignal call kill() and sets q.wakeSignal to nil if it is not nil.
//Returns true if the old wakeSignal is not nil, false otherwise.
//It should only be called when q is locked.
func (q *TimeQueue) killWakeSignal() bool {
	if q.wakeSignal != nil {
		q.wakeSignal.kill()
		q.wakeSignal = nil
		return true
	}
	return false
}

//Stop tells the running go-routine to stop running.
//This results in no more Messages being released (until a subsequent call to Start())
//and the state to be set to not running.
//If q is already stopped, then Stop is a nop.
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

//setRunning is the unexported version of SetRunning. Sets q.running to running.
//It should only be called when q is locked.
func (q *TimeQueue) setRunning(running bool) {
	q.running = running
}

//wakeSignal represents a signal that sends a time.Time value after that time has passed.
//wakeSignals can be killed, which will prevent the signal from sending its value.
type wakeSignal struct {
	dst  chan time.Time
	src  <-chan time.Time
	stop chan struct{}
}

//newWakeSignal create a wakeSignal that sends wakeTime on dst when wakeTime passes.
//this function should be used to create wakeSignals.
//the zero value wakeSignal is not valid.
func newWakeSignal(dst chan time.Time, wakeTime time.Time) *wakeSignal {
	return &wakeSignal{
		dst:  dst,
		src:  time.After(wakeTime.Sub(time.Now())),
		stop: make(chan struct{}),
	}
}

//spawn starts a new go-routine that selects on w.src and w.stop.
//If w.src is selected, the received value is sent on w.dst.
//If w.stop is selected, then the function stops selecting.
//In both cases, w.src is set to nil and the function returns.
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

//kill closes the w.stop channel.
//This is NOT idempotent. I.e. kill should only be called once a single wakeSignal.
func (w *wakeSignal) kill() {
	close(w.stop)
}
