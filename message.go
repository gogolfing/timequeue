package timequeue

import (
	"container/heap"
	"fmt"
	"time"
)

type Message struct {
	time.Time
	Data interface{}
}

func (m *Message) String() string {
	return fmt.Sprintf("&timequeue.Message%v", *m)
}

type messageHeap struct {
	messages []*Message
}

func newMessageHeap(messages ...*Message) *messageHeap {
	if messages == nil {
		messages = []*Message{}
	}
	mh := &messageHeap{
		messages: messages,
	}
	heap.Init(mh)
	return mh
}

func (mh *messageHeap) Len() int {
	return len(mh.messages)
}

func (mh *messageHeap) Less(i, j int) bool {
	return mh.messages[i].Time.Before(mh.messages[j].Time)
}

func (mh *messageHeap) Swap(i, j int) {
	mh.messages[i], mh.messages[j] = mh.messages[j], mh.messages[i]
}

func (mh *messageHeap) peekMessage() *Message {
	if mh.Len() > 0 {
		return mh.messages[0]
	}
	return nil
}

func (mh *messageHeap) pushMessage(message *Message) {
	if message == nil {
		return
	}
	heap.Push(mh, message)
}

func (mh *messageHeap) Push(value interface{}) {
	mh.messages = append(mh.messages, value.(*Message))
}

func (mh *messageHeap) popMessage() *Message {
	if mh.Len() == 0 {
		return nil
	}
	return heap.Pop(mh).(*Message)
}

func (mh *messageHeap) Pop() interface{} {
	n := len(mh.messages)
	result := (mh.messages)[n-1]
	mh.messages = (mh.messages)[0 : n-1]
	return result
}
