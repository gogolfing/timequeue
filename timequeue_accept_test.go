package timequeue_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gogolfing/timequeue"
)

func ExampleTimeQueue() {
	return

	tq := timequeue.New()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	log.Println("have context")

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)

		<-ctx.Done()

		tq.Stop()

		log.Println("ctx is Done")
	}()

	doneProducing := make(chan struct{})
	go func() {
		defer close(doneProducing)

		for i := 0; i < 10; i++ {
			m := tq.Push(time.Now().Add(time.Duration(i)*time.Second), 0, i)
			log.Println("pushed", m)
		}

		log.Println("done producing")
	}()

	doneConsuming := make(chan struct{})
	go func() {
		defer close(doneConsuming)

		for {
			select {
			case <-ctx.Done():
				return

			case m := <-tq.Messages():
				log.Println("received", m)
				fmt.Println(m.Data.(int))
			}
		}
	}()

	<-doneProducing
	<-doneConsuming
	<-stopped
}
