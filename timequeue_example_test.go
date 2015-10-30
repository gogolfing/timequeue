package timequeue_test

import (
	"fmt"
	"time"

	"github.com/gogolfing/timequeue"
)

func Example() {
	tq := timequeue.New()
	tq.Start()
	//this would normally be a long-running process,
	//and not stop at the return of a function call.
	defer tq.Stop()

	startTime := time.Now()

	immediate := &timequeue.Message{
		Time: startTime,
		Data: "this will be released immediately",
	}
	tq.PushMessage(immediate)

	//adding Messages in chronological order.
	for i := 1; i <= 4; i++ {
		tq.Push(
			startTime.Add(time.Duration(i)*time.Second),
			fmt.Sprintf("message at second %v", i),
		)
	}
	//adding Messages in reverse chronological order.
	for i := 8; i >= 5; i-- {
		tq.Push(
			startTime.Add(time.Duration(i)*time.Second),
			fmt.Sprintf("message at second %v", i),
		)
	}

	//receive all 9 Messages that were pushed.
	for i := 0; i < 9; i++ {
		message := <-tq.Messages()
		fmt.Println(message.Data)
	}

	fmt.Printf("there are %v messages left in the queue\n", tq.Size())

	endTime := time.Now()
	if endTime.Sub(startTime) > time.Duration(8)*time.Second {
		fmt.Println("releasing all messages took more than 8 seconds")
	} else {
		fmt.Println("releasing all messages took less than 8 seconds")
	}

	//Output:
	//this will be released immediately
	//message at second 1
	//message at second 2
	//message at second 3
	//message at second 4
	//message at second 5
	//message at second 6
	//message at second 7
	//message at second 8
	//there are 0 messages left in the queue
	//releasing all messages took more than 8 seconds
}
