package timequeue_test

import (
	"context"
	"fmt"
	"time"

	"github.com/gogolfing/timequeue"
)

func ExampleTimeQueue() {
	now := time.Now()
	tq := timequeue.NewTimeQueue()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)

		<-ctx.Done()

		tq.Stop()
	}()

	doneProducing := make(chan struct{})
	go func() {
		defer close(doneProducing)

		const count = 10

		toPush := make([]*timequeue.Message, count)
		for i := 0; i < count; i++ {
			m := timequeue.NewMessage(now.Add(time.Duration(i)), i+1)
			toPush[i] = m
		}

		tq.PushAll(toPush...)
	}()

	doneConsuming := make(chan struct{})
	go func() {
		defer close(doneConsuming)

		for {
			select {
			case <-stopped:
				return

			case m := <-tq.Messages():
				fmt.Println(m.Data().(int))
			}
		}
	}()

	<-doneProducing
	<-stopped
	<-doneConsuming

	//Output:
	//1
	//2
	//3
	//4
	//5
	//6
	//7
	//8
	//9
	//10
}
