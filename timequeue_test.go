package timequeue

import "testing"

func TestNew(t *testing.T) {
	q := New()
	if cap(q.messageChan) != DefaultSize {
		t.Fail()
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
