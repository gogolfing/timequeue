package timequeue

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	q := New()
	if cap(q.messageChan) != DefaultCapacity {
		t.Errorf("cap(messageChan) = %v WANT %v", cap(q.messageChan), DefaultCapacity)
	}
}

func TestNewCapacity(t *testing.T) {
	q := NewCapacity(2)
	if size := q.messages.Len(); size != 0 {
		t.Errorf("NewSize() q.messges.Len() = %v WANT %v", size, 0)
	}
	if q.lock == nil {
		t.Errorf("NewSize() lock should be non-nil")
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
	size := q.messages.Len()
	if size != 1 {
		t.Errorf("q.messages.Len() = %v WANT %v", size, 1)
	}
	if message == nil {
		t.Errorf("message = nil WANT non-nil")
	}
	if message != q.messages.peekMessage() {
		t.Errorf("return message should equal peek message")
	}
	if !message.Time.Equal(time.Time{}) {
		t.Errorf("message.Time = %v WANT %v", message.Time, time.Time{})
	}
	if message.Data != "test_data" {
		t.Errorf("message.Data = %v WANT %v", message.Data, "test_data")
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
		messageValues []*testMessageValue
		release       bool
	}{
		{[]*testMessageValue{}, false},
		{[]*testMessageValue{}, true},
		{[]*testMessageValue{{now, 0}}, false},
		{[]*testMessageValue{{now, 0}}, true},
		{[]*testMessageValue{{now, 0}, {now.Add(1), 1}, {now.Add(2), 2}}, true},
		{[]*testMessageValue{{now.Add(4), 4}, {now.Add(2), 2}, {now.Add(1), 1}, {now, 0}}, true},
	}
	for _, test := range tests {
		q := New()
		want := []*Message{}
		for _, mv := range test.messageValues {
			message := q.Push(mv.Time, mv.Data)
			want = append(want, message)
		}
		sort.Sort(&messageHeap{want})
		result := q.PopAll(test.release)
		if !areMessagesEqual(result, want) {
			t.Errorf("q.PopAll() messages sorted = %v WANT %v", result, want)
		}
		if test.release && !areChannelMessagesEqual(q.Messages(), want) {
			t.Errorf("q.PopAll() Messages() sorted WANT %v", want)
		}
		if len(q.Messages()) != 0 {
			t.Errorf("len(q.Messages() = %v WANT %v", len(q.Messages()), 0)
		}
	}
}

func TestTimeQueue_PopAllUntil(t *testing.T) {
	now := time.Now()
	tests := []struct {
		messageValues []*testMessageValue
		release       bool
		untilTime     time.Time
		untilCount    int
	}{
		{[]*testMessageValue{}, false, now.Add(10), 0},
		{[]*testMessageValue{}, true, now.Add(-10), 0},
		{[]*testMessageValue{{now, 0}}, true, now, 0},
		{[]*testMessageValue{{now, 0}, {now.Add(1), 1}, {now.Add(2), 2}}, true, now.Add(2), 2},
		{[]*testMessageValue{{now.Add(4), 4}, {now.Add(2), 2}, {now.Add(1), 1}, {now, 0}}, true, now.Add(3), 3},
		{[]*testMessageValue{{now.Add(4), 4}, {now.Add(-1), -1}, {now.Add(2), 2}, {now.Add(1), 1}, {now, 0}}, true, now.Add(3), 4},
	}
	for _, test := range tests {
		q := New()
		want := []*Message{}
		for _, mv := range test.messageValues {
			message := q.Push(mv.Time, mv.Data)
			want = append(want, message)
		}
		sort.Sort(&messageHeap{want})
		want = want[:test.untilCount]
		result := q.PopAllUntil(test.untilTime, test.release)
		if !areMessagesEqual(result, want) {
			t.Errorf("q.PopAllUntil() messages sorted = %v WANT %v", result, want)
		}
		if test.release && !areChannelMessagesEqual(q.Messages(), want) {
			t.Errorf("q.PopAllUntil() Messages() sorted WANT %v", want)
		}
		if q.messages.Len() != len(test.messageValues)-test.untilCount {
			t.Errorf("len(q.messages) = %v WANT %v", q.messages.Len(), len(test.messageValues)-test.untilCount)
		}
		if len(q.Messages()) != 0 {
			t.Errorf("len(q.Messages()) = %v WANT %v", len(q.Messages()), 0)
		}
	}
}

func TestTimeQueue_Remove_empty(t *testing.T) {
	q := New()
	if result := q.Remove(nil, true); result {
		t.Errorf("q.Remove() = %v WANT %v", result, false)
	}
	if size := len(q.Messages()); size != 0 {
		t.Errorf("len(q.Messages()) = %v WANT %v", size, 0)
	}
}

func TestTimeQueue_Remove_nonEmpty(t *testing.T) {
	tests := []struct {
		release bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		q := New()
		want := q.Push(time.Now(), nil)
		if result := q.Remove(want, test.release); !result {
			t.Errorf("q.Remove() = %v WANT %v", result, true)
		}
		if test.release {
			if actual := <-q.Messages(); actual != want {
				t.Errorf("<-q.Messages() = %v WANT %v", actual, want)
			}
		}
		if size := q.Size(); size != 0 {
			t.Errorf("t.Size() = %v WANT %v", size, 0)
		}
		if size := len(q.Messages()); size != 0 {
			t.Errorf("len(q.Messages()) = %v WANT %v", size, 0)
		}
	}
}

func TestTimeQueue_Remove_notIn(t *testing.T) {
	q := New()
	q.Push(time.Now(), nil)
	other := New().Push(time.Now(), nil)
	if result := q.Remove(other, true); result {
		t.Errorf("q.Remove(other) = %v WANT %v", result, false)
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

func TestTimeQueue_Start_notRunning(t *testing.T) {
	q := New()
	q.setRunning(true)
	q.Start()
	if q.wakeSignal != nil {
		t.Errorf("q.wakeSignal = non-nil WANT nil")
	}
}

func TestTimeQueue_Start_running(t *testing.T) {
	q := New()
	message := q.Push(time.Now().Add(time.Duration(200)*time.Millisecond), "test_data")
	q.Start()
	defer q.Stop()
	if q.wakeSignal == nil {
		t.Errorf("q.wakeSignal = nil WANT non-nil")
	}
	if running := q.IsRunning(); !running {
		t.Errorf("running = %v WANT %v", running, true)
	}
	if result := <-q.Messages(); result != message {
		t.Errorf("message = %v WANT %v", result, message)
	}
}

func TestTimeQueue_run(t *testing.T) {
	q := New()
	go func() {
		q.wakeChan <- time.Now()
		q.stopChan <- struct{}{}
	}()
	q.run()
	if q.wakeSignal != nil {
		t.Errorf("q.wakeSignal = non-nil WANT nil")
	}
	if count := len(q.messageChan); count != 0 {
		t.Errorf("len(q.messageChan) = %v WANT %v", count, 0)
	}
}

func TestTimeQueue_onWake(t *testing.T) {
	q := New()
	now := time.Now()
	for i := 0; i < 4; i++ {
		q.Push(now.Add(time.Duration(i)), i)
	}
	q.onWake(now.Add(4))
	for i := 0; i < 4; i++ {
		message := <-q.Messages()
		if message.Data != i {
			t.Errorf("message.Data = %v WANT %v", message.Data, i)
		}
	}
	if q.wakeSignal != nil {
		t.Errorf("q.wakeSignal = non-nil WANT nil")
	}
}

func TestTimeQueue_popAllUntil(t *testing.T) {
	q := New()
	now := time.Now()
	for i := 4; i >= 0; i-- {
		q.Push(now.Add(time.Duration(i)), i)
	}
	q.popAllUntil(now.Add(5), true)
	for i := 0; i <= 4; i++ {
		message := <-q.Messages()
		if message.Data != i {
			t.Errorf("message.Data = %v WANT %v", message.Data, i)
		}
	}
	if size := q.Size(); size != 0 {
		t.Errorf("q.Size() = %v WANT %v", size, 0)
	}
	if q.wakeSignal != nil {
		t.Errorf("q.wakeSignal = non-nil WANT nil")
	}
}

func TestTimeQueue_releaseMessage(t *testing.T) {
	q := New()
	q.releaseMessage(&Message{time.Now(), 0, nil, notInIndex})
	if message := <-q.Messages(); message.Data != 0 {
		t.Errorf("message.Data = %v WANT %v", message.Data, 0)
	}
}

func TestTimeQueue_releaseCopyToChan(t *testing.T) {
	tests := []struct {
		messages []*Message
	}{
		{nil},
		{[]*Message{}},
		{[]*Message{{time.Now(), 0, nil, notInIndex}, {time.Now(), 1, nil, notInIndex}}},
	}
	for _, test := range tests {
		q := New()
		q.releaseCopyToChan(test.messages)
		for _, wantMessage := range test.messages {
			if message := <-q.Messages(); message != wantMessage {
				t.Errorf("q.Messages() = %v	WANT %v", message, wantMessage)
			}
		}
	}
}

func TestTimeQueue_releaseChan(t *testing.T) {
	tests := []struct {
		messages []*Message
	}{
		{nil},
		{[]*Message{}},
		{[]*Message{{time.Now(), 0, nil, notInIndex}, {time.Now(), 1, nil, notInIndex}}},
	}
	for _, test := range tests {
		q := New()
		out := make(chan *Message)
		go func() {
			for _, message := range test.messages {
				out <- message
			}
			close(out)
		}()
		q.releaseChan(out)
		for _, wantMessage := range test.messages {
			if message := <-q.Messages(); message != wantMessage {
				t.Errorf("q.Messages() = %v	WANT %v", message, wantMessage)
			}
		}
	}
}

func TestTimeQueue_updateAndSpawnWakeSignal_empty(t *testing.T) {
	q := New()
	if result := q.updateAndSpawnWakeSignal(); result != false {
		t.Errorf("q.updateAndSpawnWakeSignal() = %v WANT %v", result, false)
	}
}

func TestTimeQueue_updateAndSpawnWakeSignal_nonEmpty(t *testing.T) {
	q := New()
	wantMessage := q.Push(time.Now().Add(time.Duration(250)*time.Millisecond), 0)
	if result := q.updateAndSpawnWakeSignal(); result != true {
		t.Fatalf("q.updateAndSpawnWakeSignal() = %v WANT %v", result, true)
	}
	if q.wakeSignal == nil {
		t.Errorf("q.wakeSignal = nil WANT non-nil")
	}
	go q.run()
	if message := <-q.Messages(); message != wantMessage {
		t.Errorf("q.Messages() = %v WANT %v", message, wantMessage)
	}
}

func TestTimeQueue_setWakeSignal(t *testing.T) {
	q := New()
	ws := newWakeSignal(q.wakeChan, time.Now())
	q.setWakeSignal(ws)
	if q.wakeSignal != ws {
		t.Errorf("q.wakeSignal = %v WANT %v", q.wakeSignal, ws)
	}
}

func TestTimeQueue_spawnWakeSignal_nil(t *testing.T) {
	q := New()
	if result := q.spawnWakeSignal(); result != false {
		t.Errorf("q.spawnWakeSignal() = %v WANT %v", result, false)
	}
}

func TestTimeQueue_spawnWakeSignal_nonNil(t *testing.T) {
	q := New()
	ws := newWakeSignal(q.wakeChan, time.Now().Add(time.Duration(1)*time.Second))
	ws.kill()
	q.setWakeSignal(ws)
	if result := q.spawnWakeSignal(); result != true {
		t.Errorf("q.spawnWakeSignal() = %v WANT %v", result, true)
	}
}

func TestTimeQueue_killWakeSignal_nil(t *testing.T) {
	q := New()
	if result := q.killWakeSignal(); result != false {
		t.Errorf("q.killWakeSignal() = %v WANT %v", result, false)
	}
}

func TestTimeQueue_killWakeSignal_nonNil(t *testing.T) {
	q := New()
	q.setWakeSignal(newWakeSignal(q.wakeChan, time.Now().Add(time.Duration(1)*time.Second)))
	if result := q.killWakeSignal(); result != true {
		t.Errorf("q.killWakeSignal() = %v WANT %v", result, true)
	}
}

func TestTimeQueue_Stop_notRunning(t *testing.T) {
	q := New()
	q.Stop()
}

func TestTimeQueue_Stop_running(t *testing.T) {
	q := New()
	q.setRunning(true)
	q.Stop()
	q.run()
	if result := q.IsRunning(); result != false {
		t.Errorf("q.IsRunning() = %v WANT %v", result, false)
	}
}

func TestTimeQueue_IsRunning(t *testing.T) {
	tests := []struct {
		value bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		q := New()
		q.running = test.value
		if result := q.IsRunning(); result != test.value {
			t.Errorf("q.IsRunning() = %v WANT %v", result, test.value)
		}
	}
}

func TestTimeQueue_setRunning(t *testing.T) {
	tests := []struct {
		value bool
	}{
		{false},
		{true},
	}
	for _, test := range tests {
		q := New()
		q.setRunning(test.value)
		if result := q.running; result != test.value {
			t.Errorf("q.running = %v WANT %v", result, test.value)
		}
	}
}

func TestNewWakeSignal(t *testing.T) {
	dst := make(chan time.Time)
	wakeTime := time.Now()
	ws := newWakeSignal(dst, wakeTime)
	if ws.dst != dst {
		t.Errorf("ws.dst = %v WANT %v", ws.dst, dst)
	}
	if ws.src == nil {
		t.Errorf("ws.src = nil WANT non-nil")
	}
	if cap(ws.src) != 1 {
		t.Errorf("cap(ws.src) = %v WANT %v", cap(ws.src), 1)
	}
	if ws.stop == nil {
		t.Errorf("ws.stop = nil WANT non-nil")
	}
	if cap(ws.stop) != 0 {
		t.Errorf("cap(ws.stop) = %v WANT %v", cap(ws.stop), 0)
	}
}

func TestWakeSignal_spawn_wake(t *testing.T) {
	dst := make(chan time.Time)
	now := time.Now()
	ws := newWakeSignal(dst, now)
	ws.spawn()
	result := <-dst
	time.Sleep(time.Duration(250) * time.Millisecond)
	diff := result.Sub(now)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Duration(1)*time.Millisecond {
		t.Errorf("<-ws.dst too far away from desired : %v WANT %v", result, now)
	}
	if ws.src != nil {
		t.Errorf("ws.src = nil WANT non-nil")
	}
}

func TestWakeSignal_spawn_stop(t *testing.T) {
	ws := newWakeSignal(nil, time.Now().Add(time.Duration(1)*time.Second))
	ws.spawn()
	ws.stop <- struct{}{}
	time.Sleep(time.Duration(250) * time.Millisecond)
	if ws.src != nil {
		t.Errorf("ws.src = nil WANT non-nil")
	}
}

func TestWakeSignal_kill(t *testing.T) {
	ws := newWakeSignal(nil, time.Now())
	ws.kill()
	defer func() {
		if result := recover(); result == nil {
			t.Errorf("kill() kill() recover() = nil WANT non-nil")
		}
	}()
	ws.kill()
}

type testMessageValue struct {
	time.Time
	Data interface{}
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
