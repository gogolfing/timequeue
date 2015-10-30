package timequeue

import (
	"container/heap"
	"fmt"
	"time"
)

//Type Message is a simple holder struct for a time.Time (the time the Message
//will be released from the queue) and a Data payload of type interface{}.
//
//A Message is not safe for modification from multiple go-routines.
//The Time field is used to calculate when the Message should be released from
//a TimeQueue, and thus changing its value while the Message is still referenced
//by a TimeQueue could have unknown side-effects.
//The Data field is never modified by a TimeQueue.
//
//It is up to client code to ensure that Data is always of the same underlying
//type if that is desired.
type Message struct {
	time.Time
	Data interface{}
}

//String returns the standard string representation of the struct.
func (m *Message) String() string {
	return fmt.Sprintf("&timequeue.Message%v", *m)
}

//messageHeap is a heap.Interface implementation for Messages.
//The peekMessage(), pushMessage(), and popMessage() methods are prefered over
//Push() and Pop() because they provide logic for emprty heaps and nil Messages.
type messageHeap struct {
	messages []*Message
}

//newMessageHeap create a pointer to messageHeap with messages added to the heap.
//heap.Init() is called before the value is returned.
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

//Len returns the number of Messages in the heap.
func (mh *messageHeap) Len() int {
	return len(mh.messages)
}

//Less determines whether or not the Message at index i is less than that at index
//j.
//This is determined by the (message at i.Time).Before(message at j.Time).
func (mh *messageHeap) Less(i, j int) bool {
	return mh.messages[i].Time.Before(mh.messages[j].Time)
}

//Swap swaps the messages at indices i and j.
func (mh *messageHeap) Swap(i, j int) {
	mh.messages[i], mh.messages[j] = mh.messages[j], mh.messages[i]
}

//peekMessage returns the "smallest" Message in the heap (without removal) or
//nil if the heap is empty.
func (mh *messageHeap) peekMessage() *Message {
	if mh.Len() > 0 {
		return mh.messages[0]
	}
	return nil
}

//pushMessage adds the Message to the heap at the approriate location in the heap.
//If message is nil, then pushMessage is a nop.
func (mh *messageHeap) pushMessage(message *Message) {
	if message == nil {
		return
	}
	heap.Push(mh, message)
}

//Push is the heap.Interface Push method that adds value to the heap.
//Appends value to the internal slice.
func (mh *messageHeap) Push(value interface{}) {
	mh.messages = append(mh.messages, value.(*Message))
}

//popMessage returns the "smallest" Message in the heap (after removal) or nil
//if the heap is empty.
func (mh *messageHeap) popMessage() *Message {
	if mh.Len() == 0 {
		return nil
	}
	return heap.Pop(mh).(*Message)
}

//Pop is the heap.Interface Pop method that removes the "smallest" Message from the heap.
func (mh *messageHeap) Pop() interface{} {
	n := len(mh.messages)
	result := (mh.messages)[n-1]
	mh.messages = (mh.messages)[0 : n-1]
	return result
}
