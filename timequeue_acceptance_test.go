package timequeue_test

import (
	"testing"
	"time"

	"github.com/gogolfing/timequeue"
)

func TestTimeQueue_acceptance_messageAddedBeforeStart(t *testing.T) {
	tq := timequeue.New()
	tq.Push(time.Now(), "now")
	tq.Start()
	defer tq.Stop()
	if message := <-tq.Messages(); message.Data != "now" {
		t.Errorf("message was not released")
	}
}

func TestTimeQueue_acceptance_startAndStopStress(t *testing.T) {
	const count = 100000
	tq := timequeue.NewCapacity(100)
	tq.Start()
	defer tq.Stop()
	for i := 0; i < count; i++ {
		tq.Push(time.Now().Add(time.Duration(i)*time.Nanosecond), i)
	}
	go func() {
		for i := 0; i < count; i++ {
			tq.Stop()
			tq.Start()
		}
	}()
	for i := 0; i < count; i++ {
		<-tq.Messages()
	}
	if size := tq.Size(); size != 0 {
		t.Errorf("size = %v WANT %v", size, 0)
	}
}

func TestTimeQueue_acceptance_millionMessagesSameTime(t *testing.T) {
	const count = 1000000
	tq := timequeue.NewCapacity(100)
	tq.Start()
	defer tq.Stop()
	now := time.Now()
	for i := 0; i < count; i++ {
		tq.Push(now, i)
	}
	for i := 0; i < count; i++ {
		<-tq.Messages()
	}
	if size := tq.Size(); size != 0 {
		t.Errorf("size = %v WANT %v", size, 0)
	}
}
