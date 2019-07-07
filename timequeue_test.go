package timequeue

import (
	"testing"
	"time"
)

func TestTimeQueue_New_CreatesAMessageChannelWithDefaultCapacity(t *testing.T) {
	tq := New()
	defer tq.Stop()

	if cap(tq.Messages()) != DefaultCapacity {
		t.Fatal()
	}
}

func TestTimeQueue_NewCapacity_CreatesAMessagesChannelWithDesiredCapacity(t *testing.T) {
	tq := NewCapacity(1234)
	defer tq.Stop()

	if cap(tq.Messages()) != 1234 {
		t.Fatal()
	}
}

func TestTimeQueue_Start_ReturnsFalseOnARunningTimeQueue(t *testing.T) {
	tq := New()
	defer tq.Stop()

	for i := 0; i < 10; i++ {
		if tq.Start() {
			t.Fatal()
		}
	}
}

func TestTimeQueue_Stop_ReturnsTrueWhileRunningAndFalseWhileStopped(t *testing.T) {
	tq := New()

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
	tq := New()
	tq.Stop()

	if !tq.Start() {
		t.Fatal()
	}

	defer tq.Stop()
}

func TestTimeQueue_ANewTimeQueueCanHoldCapacityWithoutBlocking_DrainCalledWhileRunning(t *testing.T) {
	tq := NewCapacity(10)
	defer tq.Stop()

	now := time.Now()
	for i := 0; i < 10; i++ {
		tq.Push(now, 0, i)
	}

	drained := tq.Drain()
	if len(drained) != 10 {
		t.Fatal()
	}
	assertDisassociated(t, drained...)
}

func TestTimeQueue_ANewTimeQueueCanHoldCapacityWithoutBlocking_DrainCalledWhileStopped(t *testing.T) {
	tq := NewCapacity(10)

	now := time.Now()
	for i := 0; i < 10; i++ {
		tq.Push(now, 0, i)
	}

	tq.Stop()

	drained := tq.Drain()
	if len(drained) != 10 {
		t.Fatal()
	}
	assertDisassociated(t, drained...)
}

func TestTimeQueue_Remove_CanRemoveAMessageWhileRunningAndNothingGetsDrained(t *testing.T) {
	tq := New()
	defer tq.Stop()

	m := tq.Push(time.Now().Add(time.Hour), 0, "whoami")

	if ok := tq.Remove(&m); !ok {
		t.Fatal(ok)
	}

	if len(tq.Drain()) != 0 {
		t.Fatal()
	}
}
