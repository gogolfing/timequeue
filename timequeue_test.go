package timequeue

import (
	"reflect"
	"sort"
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

func TestTimeQueue_Push(t *testing.T) {
	q := New()
	message := q.Push(time.Time{}, "test_data")
	size := q.messageHeap.Len()
	if size != 1 {
		t.Errorf("q.messageHeap.Len() = %v WANT %v", size, 1)
	}
	if message == nil {
		t.Errorf("message = nil WANT non-nil")
	}
	if message != q.messageHeap[0] {
		t.Errorf("return message should equal peel message")
	}
	if !message.Time.Equal(time.Time{}) {
		t.Errorf("message.Time = %v WANT %v", message.Time, time.Time{})
	}
	if message.Data != "test_data" {
		t.Errorf("message.Data = %v WANT %v", message.Data, "test_data")
	}
}

func TestTimeQueue_PushMessage_nil(t *testing.T) {
	q := New()
	q.PushMessage(nil)
	size := q.messageHeap.Len()
	if size != 0 {
		t.Errorf("q.messageHeap.Len() = %v WANT %v", size, 0)
	}
}

func TestTimeQueue_PushMessage_nonNil(t *testing.T) {
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

func TestTimeQueue_Peek_nil(t *testing.T) {
	q := New()
	peekTime, data := q.Peek()
	if !peekTime.IsZero() || data != nil {
		t.Errorf("q.Peek() = %v, %v WANT %v, %v", peekTime, data, time.Time{}, nil)
	}
}

func TestTimeQueue_Peek_nonNil(t *testing.T) {
	q := New()
	now := time.Now()
	q.Push(now, "test_data")
	peekTime, data := q.Peek()
	if !peekTime.Equal(now) || data != "test_data" {
		t.Errorf("q.Peek() = %v, %v WANT %v, %v", peekTime, data, now, "test_data")
	}
}

func TestTimeQueue_PeekMessage_nil(t *testing.T) {
	q := New()
	message := q.PeekMessage()
	if message != nil {
		t.Errorf("q.PeekMessage() = non-nil WANT nil")
	}
}

func TestTimeQueue_PeekMessage_nonNil(t *testing.T) {
	q := New()
	want := q.Push(time.Now(), "test_data")
	actual := q.PeekMessage()
	if actual == nil || actual != want {
		t.Errorf("q.PeekMessage() = %v WANT %v", actual, want)
	}
}

func TestTimeQueue_Pop_empty(t *testing.T) {
	q := New()
	message := q.Pop(false)
	if message != nil {
		t.Errorf("q.Pop() is non-nil WANT nil")
	}
}

func TestTimeQueue_Pop_nonEmptyRelease(t *testing.T) {
	q := New()
	want := q.Push(time.Now(), "test_data")
	actual := q.Pop(true)
	if actual != want {
		t.Errorf("q.Pop() return = %v WANT %v", actual, want)
	}
	actual = <-q.Messages()
	if actual != want {
		t.Errorf("q.Pop() Messages() = %v WANT %v", actual, want)
	}
	if len(q.Messages()) != 0 {
		t.Errorf("len(q.Messages()) = %v WANT %v", len(q.Messages()), 0)
	}
}

func TestTimeQueue_Pop_nonEmptyNonRelease(t *testing.T) {
	q := New()
	want := q.Push(time.Now(), "test_data")
	actual := q.Pop(true)
	if actual != want {
		t.Errorf("q.Pop() return = %v WANT %v", actual, want)
	}
}

func TestTimeQueue_PopAll(t *testing.T) {
	now := time.Now()
	tests := []struct {
		messages []*Message
		release  bool
	}{
		{[]*Message{}, false},
		{[]*Message{}, true},
		{[]*Message{{now, 0}}, false},
		{[]*Message{{now, 0}}, true},
		{[]*Message{{now, 0}, {now.Add(1), 1}, {now.Add(2), 2}}, true},
		{[]*Message{{now.Add(4), 4}, {now.Add(2), 2}, {now.Add(1), 1}, {now, 0}}, true},
	}
	for _, test := range tests {
		q := New()
		for _, message := range test.messages {
			q.PushMessage(message)
		}
		result := q.PopAll(test.release)
		sorted := cloneMessages(test.messages)
		sort.Sort(messageHeap(sorted))
		if !areMessagesEqual(result, sorted) {
			t.Errorf("q.PopAll() messages sorted = %v WANT %v", result, sorted)
		}
		if test.release && !areChannelMessagesEqual(q.Messages(), sorted) {
			t.Errorf("q.PopAll() Messages() sorted WANT %v", sorted)
		}
		if len(q.Messages()) != 0 {
			t.Errorf("len(q.Messages() = %v WANT %v", len(q.Messages()), 0)
		}
	}
}

func TestTimeQueue_PopAllUntil(t *testing.T) {
	now := time.Now()
	tests := []struct {
		messages   []*Message
		release    bool
		untilTime  time.Time
		untilCount int
	}{
		{[]*Message{}, false, now.Add(10), 0},
		{[]*Message{}, true, now.Add(-10), 0},
		{[]*Message{{now, 0}}, true, now, 0},
		{[]*Message{{now, 0}, {now.Add(1), 1}, {now.Add(2), 2}}, true, now.Add(2), 2},
		{[]*Message{{now.Add(4), 4}, {now.Add(2), 2}, {now.Add(1), 1}, {now, 0}}, true, now.Add(3), 3},
		{[]*Message{{now.Add(4), 4}, {now.Add(-1), -1}, {now.Add(2), 2}, {now.Add(1), 1}, {now, 0}}, true, now.Add(3), 4},
	}
	for _, test := range tests {
		q := New()
		for _, message := range test.messages {
			q.PushMessage(message)
		}
		result := q.PopAllUntil(test.untilTime, test.release)
		sorted := cloneMessages(test.messages)
		sort.Sort(messageHeap(sorted))
		sorted = sorted[:test.untilCount]
		if !areMessagesEqual(result, sorted) {
			t.Errorf("q.PopAllUntil() messages sorted = %v WANT %v", result, sorted)
		}
		if test.release && !areChannelMessagesEqual(q.Messages(), sorted) {
			t.Errorf("q.PopAllUntil() Messages() sorted WANT %v", sorted)
		}
		if len(q.messageHeap) != len(test.messages)-test.untilCount {
			t.Errorf("len(q.messageHeap) = %v WANT %v", len(q.messageHeap), len(test.messages)-test.untilCount)
		}
		if len(q.Messages()) != 0 {
			t.Errorf("len(q.Messages()) = %v WANT %v", len(q.Messages()), 0)
		}
	}
}

func TestTimeQueue_afterHeapUpdate_notRunning(t *testing.T) {
	q := New()
	q.afterHeapUpdate()
	if q.wakeSignal != nil {
		t.Errorf("q.wakeSignal = non-nil WANT nil")
	}
}

func TestTimeQueue_afterHeapUpdate_running(t *testing.T) {
	q := New()
	q.setRunning(true)
	q.afterHeapUpdate()
	if q.wakeSignal != nil {
		t.Errorf("q.wakeSignal = non-nil WANT nil")
	}
}

func TestTimeQueue_Messages(t *testing.T) {
	q := New()
	if q.Messages() != q.messageChan {
		t.Errorf("q.Messages() != q.messageChan")
	}
}

func TestTimeQueue_Size(t *testing.T) {
	q := New()
	q.Push(time.Now(), 0)
	if q.Size() != 1 {
		t.Errorf("q.Size() = %v WANT %v", q.Size(), 1)
	}
}

func cloneMessages(messages []*Message) []*Message {
	if messages == nil {
		return nil
	}
	result := make([]*Message, 0, len(messages))
	for _, message := range messages {
		result = append(result, message)
	}
	return result
}

func areChannelMessagesEqual(actualChan <-chan *Message, want []*Message) bool {
	actual := []*Message{}
	for i := 0; i < len(want); i++ {
		actual = append(actual, <-actualChan)
	}
	return areMessagesEqual(actual, want)
}

func areMessagesEqual(actual, want []*Message) bool {
	return (len(actual) == 0 && len(want) == 0) || reflect.DeepEqual(actual, want)
}
