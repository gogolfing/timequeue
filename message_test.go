package timequeue

import (
	"fmt"
	"testing"
	"time"
)

func TestMessage_String(t *testing.T) {
	now := time.Now()
	message := &Message{now, "test_data", nil, notInIndex}
	want := "&timequeue.Message{" + now.String() + " test_data}"
	if result := message.String(); result != want {
		t.Errorf("message.String() = %v WANT %v", result, want)
	}
}

func TestNewMessageHeap(t *testing.T) {
	mh := newMessageHeap()
	if mh.messages == nil {
		t.Errorf("mh.messages = nil WANT non-nil")
	}
	if size := len(mh.messages); size != 0 {
		t.Errorf("len(mh.messages) = %v WANT %v", size, 0)
	}
}

func TestMessageHeap_Len(t *testing.T) {
	tests := []struct {
		messages []*Message
		result   int
	}{
		{nil, 0},
		{[]*Message{}, 0},
		{[]*Message{{time.Now(), 0, nil, notInIndex}, {time.Now(), 1, nil, notInIndex}}, 2},
	}
	for _, test := range tests {
		if result := (&messageHeap{test.messages}).Len(); result != test.result {
			t.Errorf("messageHeap.Len() = %v WANT %v", result, test.result)
		}
	}
}

func TestMessageHeap_Less(t *testing.T) {
	now := time.Now()
	tests := []struct {
		a      *Message
		b      *Message
		result bool
	}{
		{&Message{now.Add(-1), 0, nil, notInIndex}, &Message{now, 0, nil, notInIndex}, true},
		{&Message{now, 0, nil, notInIndex}, &Message{now, 0, nil, notInIndex}, false},
		{&Message{now.Add(1), 0, nil, notInIndex}, &Message{now, 0, nil, notInIndex}, false},
	}
	for _, test := range tests {
		//do this so the heap.Init() is not called and messes with the ordering we want.
		mh := &messageHeap{
			messages: []*Message{test.a, test.b},
		}
		if result := mh.Less(0, 1); result != test.result {
			t.Errorf("mh.Less(%v, %v) = %v WANT %v", mh.messages[0], mh.messages[1], result, test.result)
		}
	}
}

func TestMessageHeap_Swap(t *testing.T) {
	mh := newMessageHeap()
	a := mh.pushMessageValues(time.Now(), 0)
	b := mh.pushMessageValues(time.Now(), 0)
	mh.Swap(0, 1)
	if mh.messages[0] != b || mh.messages[1] != a {
		t.Errorf("mh.Swap(0, 1) should equal b, a")
	}
	if a.index != 1 {
		t.Errorf("mh.Swap() a.index = %v WANT %v", a.index, 1)
	}
	if b.index != 0 {
		t.Errorf("mh.Swap() b.index = %v WANT %v", b.index, 0)
	}
}

func TestMessageHeap_Push(t *testing.T) {
	mh := newMessageHeap()
	message := &Message{time.Now(), 0, nil, notInIndex}
	mh.Push(message)
	if mh.Len() != 1 || mh.messages[0] != message {
		t.Errorf("mh.Len(), mh[0] = %v, %v WANT %v, %v", mh.Len(), 1, mh.messages[0], message)
	}
}

func TestMessageHeap_Pop(t *testing.T) {
	mh := newMessageHeap()
	message := mh.pushMessageValues(time.Now(), 0)
	if result := mh.Pop(); result != message {
		t.Errorf("mh.Pop() = %v WANT %v", result, message)
	}
	if size := mh.Len(); size != 0 {
		t.Errorf("mh.Len() = %v WANT %v", size, 0)
	}
}

func TestMessageHeap_peekMessage_empty(t *testing.T) {
	mh := newMessageHeap()
	if message := mh.peekMessage(); message != nil {
		t.Errorf("mh.peekMessage() = non-nil WANT nil")
	}
}

func TestMessageHeap_peekMessage_nonEmpty(t *testing.T) {
	mh := newMessageHeap()
	want := mh.pushMessageValues(time.Now(), nil)
	mh.pushMessageValues(time.Now(), nil)
	if actual := mh.peekMessage(); actual != want {
		t.Errorf("mh.peekMessage() = %v WANT %v", actual, want)
	}
	if size := mh.Len(); size != 2 {
		t.Errorf("mh.Len() = %v WANT %v", size, 2)
	}
}

func TestMessageHeap_pushMessageValues(t *testing.T) {
	mh := newMessageHeap()
	for i := 0; i < 10; i++ {
		data := fmt.Sprintf("data_%v", i)
		now := time.Now()
		message := mh.pushMessageValues(now, data)
		if message.Time != now {
			t.Errorf("message.Time = %v WANT %v", message.Time, now)
		}
		if message.Data != data {
			t.Errorf("message.Data = %v WANT %v", message.Data, data)
		}
		if message.mh != mh {
			t.Errorf("message.mh = %v WANT %v", message.mh, mh)
		}
		if message.index != i {
			t.Errorf("message.index = %v WANT %v", message.index, i)
		}
	}
}

func TestMessageHeap_popMessage_empty(t *testing.T) {
	mh := newMessageHeap()
	if message := mh.popMessage(); message != nil {
		t.Errorf("mh.popMessage() = non-nil WANT nil")
	}
}

func TestMessageHeap_popMessage_nonEmpty(t *testing.T) {
	mh := newMessageHeap()
	want := mh.pushMessageValues(time.Now(), 0)
	actual := mh.popMessage()
	if actual != want {
		t.Errorf("mh.popMessage() = %v WANT %v", actual, want)
	}
	if actual.mh != nil || actual.index != notInIndex {
		t.Errorf("popped message mh, index = %v, %v WANT %v, %v", actual.mh, actual.index, nil, notInIndex)
	}
	if size := mh.Len(); size != 0 {
		t.Errorf("mh.Len() = %v WANT %v", size, 0)
	}
}

func TestMessageHeap_removeMessage_empty(t *testing.T) {
	mh := newMessageHeap()
	if result := mh.removeMessage(nil); result {
		t.Errorf("mh.removeMessage() = %v WANT %v", result, false)
	}
}

func TestMessageHeap_removeMessage_messageNil(t *testing.T) {
	mh := newMessageHeap()
	mh.pushMessageValues(time.Now(), nil)
	if result := mh.removeMessage(nil); result {
		t.Errorf("mh.removeMessage() = %v WANT %v", result, false)
	}
}

func TestMessageHeap_removeMessage_notInIndex(t *testing.T) {
	mh := newMessageHeap()
	mh.pushMessageValues(time.Now(), nil)
	mh.pushMessageValues(time.Now(), nil)
	message := mh.popMessage()
	if result := mh.removeMessage(message); result {
		t.Errorf("mh.removeMessage() = %v WANT %v", result, false)
	}
}

func TestMessageHeap_removeMessage_notInMh(t *testing.T) {
	mh := newMessageHeap()
	mh.pushMessageValues(time.Now(), nil)
	other := newMessageHeap().pushMessageValues(time.Now(), nil)
	if result := mh.removeMessage(other); result {
		t.Errorf("mh.removeMessage() = %v WANT %v", result, false)
	}
}

func TestMessageHeap_removeMessage_success(t *testing.T) {
	mh := newMessageHeap()
	message := mh.pushMessageValues(time.Now(), nil)
	if result := mh.removeMessage(message); !result {
		t.Errorf("mh.removeMessage() = %v WANT %v", result, true)
	}
	if size := mh.Len(); size != 0 {
		t.Errorf("mh.Len() = %v WANT %v", size, 0)
	}
	if message.mh != nil || message.index != notInIndex {
		t.Errorf("popped message mh, index = %v, %v WANT %v, %v", message.mh, message.index, nil, notInIndex)
	}
}

func TestBeforeRemoval(t *testing.T) {
	mh := newMessageHeap()
	message := &Message{time.Now(), nil, mh, 1}
	beforeRemoval(message)
	if message.mh != nil {
		t.Errorf("message.mh = non-nil WANT nil")
	}
	if message.index != notInIndex {
		t.Errorf("message.index = %v WANT %v", message.index, notInIndex)
	}
}
