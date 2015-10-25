package timequeue

import (
	"testing"
	"time"
)

func TestMessage_String(t *testing.T) {
	now := time.Now()
	message := &Message{now, "test_data"}
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
		{[]*Message{{time.Now(), 0}, {time.Now(), 1}}, 2},
	}
	for _, test := range tests {
		if result := messageHeap(test.messages).Len(); result != test.result {
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
		{&Message{now.Add(-1), 0}, &Message{now, 0}, true},
		{&Message{now, 0}, &Message{now, 0}, false},
		{&Message{now.Add(1), 0}, &Message{now, 0}, false},
	}
	for _, test := range tests {
		mh := messageHeap([]*Message{test.a, test.b})
		if result := mh.Less(0, 1); result != test.result {
			t.Errorf("mh.Less(%v, %v) = %v WANT %v", mh[0], mh[1], result, test.result)
		}
	}
}

func TestMessageHeap_Swap(t *testing.T) {
	a := &Message{time.Now(), 0}
	b := &Message{time.Now(), 0}
	mh := messageHeap([]*Message{a, b})
	mh.Swap(0, 1)
	if mh[0] != b || mh[1] != a {
		t.Errorf("mh.Swap(0, 1) should equal b, a")
	}
}

func TestMessageHeap_Push(t *testing.T) {
	mh := messageHeap([]*Message{})
	message := &Message{time.Now(), 0}
	(&mh).Push(message)
	if mh.Len() != 1 || mh[0] != message {
		t.Errorf("mh.Len(), mh[0] = %v, %v WANT %v, %v", mh.Len(), 1, mh[0], message)
	}
}

func TestMessageHeap_Pop(t *testing.T) {
	message := &Message{time.Now(), 0}
	mh := messageHeap([]*Message{message})
	if result := mh.Pop(); result != message {
		t.Errorf("mh.Pop() = %v WANT %v", result, message)
	}
	if size := mh.Len(); size != 0 {
		t.Errorf("mh.Len() = %v WANT %v", size, 0)
	}

}
