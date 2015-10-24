package timequeue

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	q := New()
	if cap(q.messageChan) != DefaultSize {
		t.Errorf("cap(messageChan) = %v WANT %v", cap(q.messageChan), DefaultSize)
	}
}

func TestNewSize(t *testing.T) {
	q := NewSize(2)
	if q.messageLock == nil || q.messageHeap == nil {
		t.Errorf("NewSize() messageLock and messageHeap should be non-nil")
	}
	if q.messageHeap.Len() != 0 {
		t.Errorf("NewSize() len(messageHeap) = %v WANT %v", len(q.messageHeap), 0)
	}
	if q.stateLock == nil {
		t.Errorf("NewSize() stateLock should be non-nil")
	}
	if q.running == true {
		t.Errorf("NewSize() running = %v WANT %v", q.running, false)
	}
	if q.wakeSignal != nil {
		t.Errorf("NewSize() wakeSignal should be nil")
	}
	if cap(q.messageChan) != 2 {
		t.Errorf("NewSize() cap(messageChan) = %v WANT %v", cap(q.messageChan), 2)
	}
	if q.wakeChan == nil || q.stopChan == nil {
		t.Errorf("NewSize() wakeChan and stopChan should be non-nil")
	}
}

func TestPush(t *testing.T) {
	q := New()
	q.Push(time.Time{}, "test_data")
	size := q.messageHeap.Len()
	if size != 1 {
		t.Errorf("q.messageHeap.Len() = %v WANT %v", size, 1)
	}
	message := q.messageHeap[0]
	if message == nil {
		t.Errorf("message = nil WANT non-nil")
	}
	if !message.Time.Equal(time.Time{}) {
		t.Errorf("message.Time = %v WANT %v", message.Time, time.Time{})
	}
	if message.Data != "test_data" {
		t.Errorf("message.Data = %v WANT %v", message.Data, "test_data")
	}
}

func TestPushMessage_nil(t *testing.T) {
	q := New()
	q.PushMessage(nil)
	size := q.messageHeap.Len()
	if size != 0 {
		t.Errorf("q.messageHeap.Len() = %v WANT %v", size, 0)
	}
}

func TestPushMessage_nonNil(t *testing.T) {
	q := New()
	message := &Message{
		Time: time.Time{},
		Data: "test_data",
	}
	q.PushMessage(message)
	size := q.messageHeap.Len()
	if size != 1 {
		t.Errorf("q.messageHeap.Len() = %v WANT %v", size, 1)
	}
	if q.messageHeap[0] != message {
		t.Errorf("q.messageHeap[0] = %v WANT %v", q.messageHeap[0], message)
	}
}
