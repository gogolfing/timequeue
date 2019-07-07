package timequeue

import (
	"sync"
	"time"
)

const (
	DefaultCapacity = 0
)

type TimeQueue struct {
	timer *time.Timer

	out chan Message

	lock        *sync.Mutex
	messageHeap messageHeap
	stopChan    chan chan struct{}
	pauseChan   chan chan struct{}
}

func NewTimeQueue() *TimeQueue {
	return NewTimeQueueCapacity(DefaultCapacity)
}

func NewTimeQueueCapacity(c int) *TimeQueue {
	tq := &TimeQueue{
		timer:       newExpiredTimer(),
		out:         make(chan Message, c),
		lock:        &sync.Mutex{},
		messageHeap: messageHeap([]*Message{}),
		stopChan:    nil,
		pauseChan:   make(chan chan struct{}), //Must not have capacity to ensure only only goroutine is able to pause the run loop.
	}

	tq.Start()

	return tq
}

func newExpiredTimer() *time.Timer {
	timer := time.NewTimer(0)
	<-timer.C

	return timer
}

func (tq *TimeQueue) Messages() <-chan Message {
	return tq.out
}

func (tq *TimeQueue) Start() bool {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	return tq.start()
}

func (tq *TimeQueue) start() bool {
	if !tq.isStopped() {
		return false
	}

	tq.stopChan = make(chan chan struct{})
	tq.run()
	return true
}

func (tq *TimeQueue) run() {
	go func() {
		for {
			select {
			case <-tq.timer.C:
				tq.releaseNextMessage()

			case resultChan := <-tq.pauseChan:
				resultChan <- struct{}{}
				<-resultChan

			case resultChan := <-tq.stopChan:
				resultChan <- struct{}{}
				return
			}

			select {
			case resultChan := <-tq.stopChan:
				resultChan <- struct{}{}
				return

			default:
			}
		}
	}()
}

func (tq *TimeQueue) releaseNextMessage() {
	//TODO document how we are the only goroutine with access to the messageHeap.

	m := popMessage(&tq.messageHeap)
	tq.dispatch(m)

	tq.maybeResetTimerToHead()
}

func (tq *TimeQueue) dispatch(m *Message) {
	//We don't need to call m.withoutHeap becuase the prior pop operation already does that.
	tq.out <- *m
}

func (tq *TimeQueue) Stop() bool {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	return tq.stop()
}

func (tq *TimeQueue) stop() bool {
	if tq.isStopped() {
		return false
	}

	resultChan := make(chan struct{})
	tq.stopChan <- resultChan
	<-resultChan

	tq.stopChan = nil
	return true
}

func (tq *TimeQueue) Drain() []Message {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	return tq.drain()
}

func (tq *TimeQueue) drain() []Message {
	unpause := tq.pause()
	defer unpause()

	if tq.messageHeap.Len() > 0 {
		defer tq.stopTimer()
	}

	//We start with the drained Messages from our heap.
	result := tq.messageHeap.drain()

	//If there are Messages on our output channel, then drain the channel.
	//Messages on this channel are already disassociated with a heap.
	for len(tq.out) > 0 {
		result = append(result, <-tq.out)
	}

	return result
}

func (tq *TimeQueue) isStopped() bool {
	return tq.stopChan == nil
}

func (tq *TimeQueue) Remove(m *Message) bool {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	return tq.remove(m)
}

func (tq *TimeQueue) remove(m *Message) bool {
	unpause := tq.pause()
	defer unpause()

	isHead := m.isHead()
	ok := tq.messageHeap.remove(m)

	if ok && isHead {
		tq.stopTimer()
		tq.maybeResetTimerToHead()
	}

	return ok
}

func (tq *TimeQueue) Push(at time.Time, data interface{}) *Message {
	m := NewMessage(at, data)
	tq.PushAll(m)
	return m
}

func (tq *TimeQueue) PushAll(messages ...*Message) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	unpause := tq.pause()
	defer unpause()

	hasNewHead := false

	for _, m := range messages {
		pushMessage(&tq.messageHeap, m)

		if m.isHead() {
			hasNewHead = true
		}
	}

	if hasNewHead {
		if len(messages) < tq.messageHeap.Len() {
			//TODO displaced docs
			tq.stopTimer()
		}
		tq.maybeResetTimerToHead()
	}
}

func (tq *TimeQueue) pause() func() {
	if tq.isStopped() {
		return func() {}
	}

	resultChan := make(chan struct{})
	tq.pauseChan <- resultChan
	<-resultChan
	return func() {
		resultChan <- struct{}{}
	}
}

func (tq *TimeQueue) stopTimer() {
	if !tq.timer.Stop() {
		<-tq.timer.C
	}
}

func (tq *TimeQueue) maybeResetTimerToHead() {
	peeked := tq.messageHeap.peek()

	if peeked != nil {
		tq.resetTimerTo(peeked.At())
	}
}

func (tq *TimeQueue) resetTimerTo(t time.Time) {
	tq.timer.Reset(time.Until(t))
}
