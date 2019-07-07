package timequeue

import (
	"testing"
	"time"
)

func TestTimeQueue_New_CreatesAMessageChannelWithDefaultCapacity(t *testing.T) {
	tq := NewTimeQueue()
	defer tq.Stop()

	if cap(tq.Messages()) != DefaultCapacity {
		t.Fatal()
	}
}

func TestTimeQueue_NewCapacity_CreatesAMessagesChannelWithDesiredCapacity(t *testing.T) {
	tq := NewTimeQueueCapacity(1234)
	defer tq.Stop()

	if cap(tq.Messages()) != 1234 {
		t.Fatal()
	}
}

func TestTimeQueue_Start_ReturnsFalseOnARunningTimeQueue(t *testing.T) {
	tq := NewTimeQueue()
	defer tq.Stop()

	for i := 0; i < 10; i++ {
		if tq.Start() {
			t.Fatal()
		}
	}
}

func TestTimeQueue_Stop_ReturnsTrueWhileRunningAndFalseWhileStopped(t *testing.T) {
	tq := NewTimeQueue()

	if !tq.Stop() {
		t.Fatal("first Stop")
	}

	for i := 0; i < 10; i++ {
		if tq.Stop() {
			t.Fatal()
		}
	}
}

func TestTimeQueue_Start_ReturnsTrueWhileStopped(t *testing.T) {
	tq := NewTimeQueue()
	tq.Stop()

	if !tq.Start() {
		t.Fatal()
	}

	defer tq.Stop()
}

func TestTimeQueue_ANewTimeQueueCanHoldCapacityWithoutBlocking_DrainCalledWhileRunning(t *testing.T) {
	tq := NewTimeQueueCapacity(10)
	defer tq.Stop()

	now := time.Now()
	for i := 0; i < 10; i++ {
		tq.Push(now, i)
	}

	drained := tq.Drain()
	if len(drained) != 10 {
		t.Fatal()
	}
	assertDisassociated(t, drained...)
}

func TestTimeQueue_ANewTimeQueueCanHoldCapacityWithoutBlocking_DrainCalledWhileStopped(t *testing.T) {
	tq := NewTimeQueueCapacity(10)

	now := time.Now()
	for i := 0; i < 10; i++ {
		tq.Push(now, i)
	}

	tq.Stop()

	drained := tq.Drain()
	if len(drained) != 10 {
		t.Fatal()
	}
	assertDisassociated(t, drained...)
}

func TestTimeQueue_Remove_CanRemoveAMessageWhileRunningAndNothingGetsDrained(t *testing.T) {
	tq := NewTimeQueue()
	defer tq.Stop()

	m := tq.Push(time.Now().Add(time.Hour), nil)

	if ok := tq.Remove(m); !ok {
		t.Fatal(ok)
	}

	if len(tq.Drain()) != 0 {
		t.Fatal()
	}
}

func TestTimeQueue_Remove_CanRemoveAMessageWhileStoppedAndNothingGetsDrained(t *testing.T) {
	tq := NewTimeQueue()

	m := tq.Push(time.Now().Add(time.Hour), nil)

	tq.Stop()

	if ok := tq.Remove(m); !ok {
		t.Fatal(ok)
	}

	if len(tq.Drain()) != 0 {
		t.Fatal()
	}
}

func TestTimeQueue_Remove_CallingRemoveWithAMessageFromAnotherQueueReturnsFalse(t *testing.T) {
	tq1 := NewTimeQueueCapacity(1) //We need capacity so we don't block.
	defer tq1.Stop()

	tq2 := NewTimeQueueCapacity(1) //We need capacity so we don't block.
	defer tq2.Stop()

	tq1.Push(time.Now(), nil)
	m2 := tq2.Push(time.Now(), nil)

	if tq1.Remove(m2) {
		t.Fatal()
	}

	if len(tq1.Drain()) != 1 {
		t.Fatal()
	}
	if len(tq2.Drain()) != 1 {
		t.Fatal()
	}
}

func TestTimeQueue_Push_WeCanPushAsManyMessagesAsWeWantAtNowWhileStoppedWithoutCapacity(t *testing.T) {
	tq := NewTimeQueue()
	tq.Stop()

	now := time.Now()
	for i := 0; i < 100; i++ {
		tq.Push(now, i)
	}
}

func TestTimeQueue_PushAll_WeCanPushAllWithAsManyMessagesAsWeWantAtNowWhileRunningWithoutCapacity(t *testing.T) {
	done := make(chan struct{})

	tq := NewTimeQueue()
	defer close(done)
	defer tq.Stop()

	now := time.Now()
	messages := []*Message{}
	for i := 0; i < 100; i++ {
		messages = append(messages, NewMessage(now, i))
	}

	tq.PushAll(messages...)

	go func() {
		for {
			select {
			case <-tq.Messages():

			case <-done:
				return
			}
		}
	}()
}

func TestTimeQueue_PushAll_CorrectlyStopsThenResetsTheTimerWhenWeAddANewHeadWithMessagesAlreadyInTheQueue(t *testing.T) {
	tq := NewTimeQueueCapacity(2) //We need capacity so we don't block.

	defer tq.Stop() //Need to be running to check the timer stop. So defer.

	now := time.Now()
	tq.PushAll(
		NewMessage(now.Add(time.Hour), nil),
	)

	tq.PushAll(
		NewMessage(now, nil),
	)
}

func TestTimeQueue_CanSuccessfullyReceiveMessageFromTimerAfterResumingFromStopped(t *testing.T) {
	tq := NewTimeQueueCapacity(1)

	tq.Stop()

	tq.Push(time.Now().Add(time.Second/2), "my message")

	tq.Start()

	m := <-tq.Messages()
	if m.Data().(string) != "my message" {
		t.Fatal()
	}
}
