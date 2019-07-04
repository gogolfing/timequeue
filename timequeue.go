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

func New() *TimeQueue {
	return NewCapacity(DefaultCapacity)
}

func NewCapacity(c int) *TimeQueue {
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
	//TODO need to test that the timer chan keeps values across stops and starts.
	//

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

	peeked := tq.messageHeap.peek()
	if peeked != nil {
		tq.resetTimerTo(peeked.At)
	}
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

	result := make([]Message, tq.messageHeap.Len())
	for i, m := range tq.messageHeap {
		result[i] = m.withoutHeap()
	}

	for len(tq.out) > 0 {
		result = append(result, <-tq.out)
	}

	return result
}

func (tq *TimeQueue) isStopped() bool {
	return tq.stopChan == nil
}

func (tq *TimeQueue) Remove(m Message) bool {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	return tq.remove(m)
}

func (tq *TimeQueue) remove(m Message) bool {
	unpause := tq.pause()
	defer unpause()

	return tq.messageHeap.remove(&m)
}

func (tq *TimeQueue) Push(at time.Time, p Priority, data interface{}) Message {
	m := NewMessage(at, p, data)
	tq.PushAll(m)
	return m
}

func (tq *TimeQueue) PushAll(messages ...Message) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	unpause := tq.pause()
	defer unpause()

	var newHead *Message

	for _, m := range messages {
		pushMessage(&tq.messageHeap, &m)

		if m.isHead() {
			newHead = &m
		}
	}

	if newHead != nil {
		if tq.messageHeap.Len() == 1 {
			//We are the new head, but the only Message, so just set timer.
			tq.resetTimerTo(newHead.At)
		} else {
			//We bumped out a prior head Message, so stop then reset.
			tq.stopTimer()
			tq.resetTimerTo(newHead.At)
		}
	}
}

func (tq *TimeQueue) pause() func() {
	resultChan := make(chan struct{})
	tq.pauseChan <- resultChan
	<-tq.pauseChan

	return func() {
		resultChan <- struct{}{}
	}
}

func (tq *TimeQueue) stopTimer() {
	if !tq.timer.Stop() {
		<-tq.timer.C
	}
}

func (tq *TimeQueue) resetTimerTo(t time.Time) {
	tq.timer.Reset(time.Until(t))
}
