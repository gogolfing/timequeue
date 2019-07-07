package timequeue

import (
	"container/heap"
	"time"
)

const (
	//indexNotInHeap is a sentinel for a Message.index that indicates the Message
	//is not a in a messageHeap.
	indexNotInHeap = -1
)

//Priority is the priority of a Message.
//Smaller values indicate a higher priority, with 0 being the highest priority.
type Priority uint32

//Message is a container type that associates a Time and Priority with some
//arbitrary data.
//A Message is "released" from a TimeQueue as close to Time At as possible.
//
//Message zero values are not in a valid state. You should use NewMessage to create
//Message instances.
type Message struct {
	//At is the Time at which to release this Message.
	At time.Time

	//Priority is the Priority of this Message.
	//If a Message has an equal At value with another Message in the same TimeQueue,
	//then Priority is consulted to "order" the Messages to determine which should
	//be "released" first.
	Priority

	//Data is any arbitrary data that you can put in a Message and retrieve when
	//the Message is released.
	Data interface{}

	//messageHeap is the messageHeap that this Message is in.
	//A nil value means that Message is not in a messageHeap.
	*messageHeap

	//index is the index at which this Message resides in messageHeap.
	index int
}

//NewMessage returns a Message with at, p, and data set on their corresponding fields.
//
//You should use this function to create Messages instead of using a struct initializer.
func NewMessage(at time.Time, p Priority, data interface{}) *Message {
	return &Message{
		At:          at,
		Priority:    p,
		Data:        data,
		messageHeap: nil,
		index:       indexNotInHeap,
	}
}

//less returns whether or not m is "less than" other.
//This is used to determined the order in which Messages are released from a TimeQueue.
//
//It returns true if m.At is before other.At, regardless of Priorities.
//If m and other have an equal At field, then true is returned if m has a lower
//Priority than other.
func (m *Message) less(other *Message) bool {
	diff := m.At.Sub(other.At)
	if diff != 0 {
		return diff < 0
	}
	return m.Priority < other.Priority
}

//isHead returns whether or not m is at the head of a messageHeap, i.e. the next
//one to be released.
func (m *Message) isHead() bool {
	return m.messageHeap != nil && m.index == 0
}

func (m *Message) withoutHeap() Message {
	m.messageHeap = nil
	m.index = indexNotInHeap
	return *m
}

//messageHeap is a slice of Messages with methods that satisfy the heap.Interface.
//
//messageHeaps can be used with the heap package to push and pop Messages ordered
//by Message.less.
//
//messageHeaps are not safe for use by multiple goroutines.
//
//We let Go manage how increasing size and capacity works when appending to a
//messageHeap.
type messageHeap []*Message

//Len is the heap.Interface implementation.
//It returns len(mh).
func (mh messageHeap) Len() int {
	return len(mh)
}

//Less is the heap.Interface implementation.
func (mh messageHeap) Less(i, j int) bool {
	return mh[i].less(mh[j])
}

//Swap is the heap.Interface implementation.
func (mh messageHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].index = i
	mh[j].index = j
}

//pushMessage is a helper that calls the heap.Push package function with mh and m.
func pushMessage(mh *messageHeap, m *Message) {
	heap.Push(mh, m)
}

//Push is the heap.Interface implementation.
func (mh *messageHeap) Push(x interface{}) {
	n := len(*mh)
	m := x.(*Message)
	m.messageHeap, m.index = mh, n
	*mh = append(*mh, m)
}

//popMessage is a helper that calls the heap.Pop package function with mh.
func popMessage(mh *messageHeap) *Message {
	return heap.Pop(mh).(*Message)
}

//Pop is the heap.Interface implementation.
func (mh *messageHeap) Pop() interface{} {
	old := *mh
	n := len(old)
	m := old[n-1]
	m.messageHeap, m.index = nil, indexNotInHeap
	*mh = old[0 : n-1]
	return m
}

//peek returns the next Message to be released, or nil if mh is empty.
func (mh *messageHeap) peek() *Message {
	if mh.Len() > 0 {
		return (*mh)[0]
	}
	return nil
}

//remove attemps to remove m from mh.
//
//It returns true if m is actually stored in mh and was actually removed, false
//if m is not in mh.
func (mh *messageHeap) remove(m *Message) bool {
	if m.messageHeap != mh {
		return false
	}

	heap.Remove(mh, m.index)
	return true
}

func (mh *messageHeap) drain() []Message {
	old := *mh

	result := make([]Message, len(old))
	for i, m := range old {
		result[i] = m.withoutHeap()
	}

	*mh = old[0:0]

	return result
}
