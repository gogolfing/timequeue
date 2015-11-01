# timequeue
Package timequeue provides the TimeQueue type that is a queue of Messages.
Each Message contains a time.Time that determines the time at which the Message
should be released from the queue.
Message types also have a Data field of type interface{} that should be used
as the payload of the Message.
TimeQueue is safe for use by multiple go-routines.

Messages need only be pushed to the queue, and then when their time passes,
they will be sent on the channel returned by Messages().
See below for examples.

TimeQueue uses a single go-routine, spawned from Start() that returns from Stop(),
that processes the Messages as their times pass.
When a Message is pushed to the queue, the earliest Message in the queue is
used to determine the next time the running go-routine should wake.
The running go-routine knows when to wake because the earliest time is used
to make a channel via time.After(). Receiving on that channel wakes the
running go-routine if a call to Stop() has not happened prior.
Upon waking, that Message is removed from the queue and released on the channel
returned from Messages().
Then the newest remaining Message is used to determine when to wake, etc.
If a Message with a time before any other in the queue is inserted, then that
Message is pushed to the front of the queue and released appropriately.

Messages that are "released", i.e. sent on the Messages() channel, are always
released from a newly spawned go-routine so that other go-routines are not
paused waiting for a receive from Messages().

Messages with the same Time value will be "flood-released" from the same
separately spawned go-routine.
Additionally, Messages that are pushed with times before time.Now() will
immediately be released from the queue.

### Documentation
Full documentation and examples can be found here:
[![GoDoc](https://godoc.org/github.com/gogolfing/timequeue?status.svg)](https://godoc.org/github.com/gogolfing/timequeue)

##Example Usage
```go
package main

import (
	"fmt"
	"time"

	"github.com/gogolfing/timequeue"
)

func main() {
	tq := timequeue.New()
	tq.Start()
	//this would normally be a long-running process,
	//and not stop at the return of a function call.
	defer tq.Stop()

	startTime := time.Now()

	tq.Push(startTime, "this will be released immediately")

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
}
```

The above results in the following output:
```
this will be released immediately
message at second 1
message at second 2
message at second 3
message at second 4
message at second 5
message at second 6
message at second 7
message at second 8
there are 0 messages left in the queue
releasing all messages took more than 8 seconds
```

## Status
[![Coverage Status](https://coveralls.io/repos/gogolfing/timequeue/badge.svg?branch=master&service=github)](https://coveralls.io/github/gogolfing/timequeue?branch=master)
[![Build Status](https://travis-ci.org/gogolfing/timequeue.svg)](https://travis-ci.org/gogolfing/timequeue)
