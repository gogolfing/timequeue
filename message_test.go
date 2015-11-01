package timequeue

import (
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
		if result := newMessageHeap(test.messages...).Len(); result != test.result {
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
	message := &Message{time.Now(), 0, nil, notInIndex}
	mh := newMessageHeap(message)
	if result := mh.Pop(); result != message {
		t.Errorf("mh.Pop() = %v WANT %v", result, message)
	}
	if size := mh.Len(); size != 0 {
		t.Errorf("mh.Len() = %v WANT %v", size, 0)
	}

}
