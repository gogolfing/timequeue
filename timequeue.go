package timequeue

import (
	"log"
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
				log.Println("got timer")
				tq.releaseNextMessage()

			case resultChan := <-tq.pauseChan:
				log.Println("got pause request")
				resultChan <- struct{}{}
				<-resultChan
				log.Println("ended pause request")

			case resultChan := <-tq.stopChan:
				log.Println("got stop request")
				resultChan <- struct{}{}
				log.Println("ended stop request")
				return
			}

			select {
			case resultChan := <-tq.stopChan:
				log.Println("got stop request")
				resultChan <- struct{}{}
				log.Println("ended stop request")
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

	//TODO something with checking timer if removed message is head.
	//TODO make sure the calling code gets understands that m is removed.

	isHead := m.isHead()
	ok := tq.messageHeap.remove(m)

	if ok && isHead {
		tq.stopTimer()
		tq.maybeResetTimerToHead()
	}

	return ok
}

func (tq *TimeQueue) Push(at time.Time, p Priority, data interface{}) Message {
	m := NewMessage(at, p, data)
	tq.PushAll(m)
	return *m
}

func (tq *TimeQueue) PushAll(messages ...*Message) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	log.Println("pushing messages", messages)

	unpause := tq.pause()
	defer unpause()

	log.Println("paused and defered unpause")

	var newHead *Message

	for _, m := range messages {
		pushMessage(&tq.messageHeap, m)

		log.Println("pushed message")

		if m.isHead() {
			log.Println("new message is head")
			newHead = m
		} else {
			log.Println("new message is NOT head")
		}
	}

	if newHead != nil {
		log.Println("doing something with timer because of new head")

		if tq.messageHeap.Len() == 1 {
			//We are the new head, but the only Message, so just set timer.
			tq.resetTimerTo(newHead.At)
		} else {
			//We bumped out a prior head Message, so stop then reset.
			tq.stopTimer()
			tq.resetTimerTo(newHead.At)
		}
	}

	log.Println("end of PushAll")
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
		tq.resetTimerTo(peeked.At)
	}
}

func (tq *TimeQueue) resetTimerTo(t time.Time) {
	tq.timer.Reset(time.Until(t))
}
