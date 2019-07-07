package timequeue_test

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/gogolfing/timequeue"
)

func messagesLessFunc(messages []timequeue.Message) func(i, j int) bool {
	return func(i, j int) bool {
		return messages[i].At().Before(messages[j].At())
	}
}

func timeWithinDurationFunc(t time.Time, d time.Duration) func() time.Time {
	return func() time.Time {
		return t.Add(time.Duration(rand.Int63n(int64(d))))
	}
}

func TestTimeQueue_SinglePublisherAndConsumerRetrievesMessagesInOrder(t *testing.T) {
	tq := timequeue.NewTimeQueue()

	const count = 10000

	go func() {
		now := time.Now()
		atFunc := timeWithinDurationFunc(now, time.Second)

		toPush := make([]*timequeue.Message, count)
		for i := 0; i < count; i++ {
			toPush[i] = timequeue.NewMessage(atFunc(), 0, i)
		}

		tq.PushAll(toPush...)
	}()

	messages := make([]timequeue.Message, 0, count)

	for i := 0; i < count; i++ {
		messages = append(messages, <-tq.Messages())
	}

	sorted := sort.SliceIsSorted(messages, messagesLessFunc(messages))
	if !sorted {
		t.Fatal(sorted)
	}
}

func TestTimeQueue_FullOnUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping for time")
	}

	//now is used a reference for the start time of the test.
	now := time.Now()
	goroutineCount := 10
	messagesPerGoroutine := 10000
	duration := time.Duration(10) * time.Second
	atFunc := timeWithinDurationFunc(now, duration)
	pauseCount := 100

	tq := timequeue.NewTimeQueueCapacity(messagesPerGoroutine)

	consumingCtx, consumingCtxCancel := context.WithCancel(context.Background())
	consumedCountChan := consumeMessages(consumingCtx, tq, goroutineCount)

	producingCtx, producingCtxCancel := context.WithCancel(context.Background())
	producedCountChan, messagesToRemove := produceMessages(
		producingCtx,
		tq,
		goroutineCount,
		messagesPerGoroutine,
		atFunc,
		0.15,
	)

	removingCtx := producingCtx
	removedCountChan := removeMessages(removingCtx, tq, 1, messagesToRemove)

	//Wait to get through a portion of the allotted duration.
	deadline := now.Add(duration * 3 / 4)

	stopCtx, stop := context.WithDeadline(context.Background(), deadline)
	defer stop()

	//Send intermittent Stop then Start signals.
	pauseThroughoutDeadline(stopCtx, tq, deadline, pauseCount)

	<-stopCtx.Done()     //Actually wait until the deadline hits.
	producingCtxCancel() //Stop producing. We may already be done producing depending on the number of messages sent.

	producedCount := <-producedCountChan
	//Getting here means that we are done producing. All producers have returned.

	tq.Stop()            //We call Stop while there are still consumers running. This is a requirement.
	consumingCtxCancel() //Now we stop consuming.

	removedCount := <-removedCountChan   //All removers have returned.
	consumedCount := <-consumedCountChan //All consumers have returned. We know there are no more receive attempts from tq.Messages().

	//From above, we know we can call Drain().

	drainedCount := len(tq.Drain())

	totalReceived := removedCount + consumedCount + drainedCount

	if producedCount != totalReceived {
		t.Fatalf(
			"produced %v ; removed + consumed + drained = %v + %v + %v = %v",
			producedCount,
			removedCount,
			consumedCount,
			drainedCount,
			totalReceived,
		)
	}
}

func consumeMessages(ctx context.Context, tq *timequeue.TimeQueue, grc int) <-chan int {
	aggChan := make(chan int)

	for i := 0; i < grc; i++ {
		go func() {
			count := 0
			for {
				select {
				case <-ctx.Done():
					aggChan <- count
					return

				case <-tq.Messages():
					count++
				}
			}
		}()
	}

	result := make(chan int, 1)

	go func() {
		defer close(aggChan)
		defer close(result)

		sum := 0
		for i := 0; i < grc; i++ {
			sum += <-aggChan
		}
		result <- sum
	}()

	return result
}

func produceMessages(ctx context.Context, tq *timequeue.TimeQueue, grc, mpg int, atFunc func() time.Time, removeRate float64) (<-chan int, <-chan *timequeue.Message) {
	aggChan := make(chan int)
	removeChan := make(chan *timequeue.Message, mpg)

	for i := 0; i < grc; i++ {
		go func() {
			count := 0

		loop:
			for count < mpg {
				select {
				case <-ctx.Done():
					break loop

				default:
					m := tq.Push(atFunc(), 0, i)
					count++

					if rand.Float64() < removeRate {
						removeChan <- m
					}
				}
			}

			aggChan <- count
		}()
	}

	result := make(chan int, 1)

	go func() {
		defer close(aggChan)
		defer close(removeChan)
		defer close(result)

		sum := 0
		for i := 0; i < grc; i++ {
			sum += <-aggChan
		}
		result <- sum
	}()

	return result, removeChan
}

func removeMessages(ctx context.Context, tq *timequeue.TimeQueue, grc int, toRemove <-chan *timequeue.Message) <-chan int {
	aggChan := make(chan int)

	for i := 0; i < grc; i++ {
		go func() {
			count := 0

		loop:
			for m := range toRemove {
				select {
				case <-ctx.Done():
					break loop

				default:
					if ok := tq.Remove(m); ok {
						count++
					}
				}
			}

			aggChan <- count
		}()
	}

	result := make(chan int, 1)

	go func() {
		defer close(aggChan)
		defer close(result)

		sum := 0
		for i := 0; i < grc; i++ {
			sum += <-aggChan
		}
		result <- sum
	}()

	return result
}

func pauseThroughoutDeadline(ctx context.Context, tq *timequeue.TimeQueue, deadline time.Time, count int) {
	defer tq.Start()

	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Until(deadline) / time.Duration(count))
		defer ticker.Stop()

		start := false

		for {
			select {
			case <-ctx.Done():
				close(done)
				return

			case <-ticker.C:
				if start {
					tq.Start()
				} else {
					tq.Stop()
				}
				start = !start
			}
		}
	}()

	<-done
}
