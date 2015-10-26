//Package timequeue provides the TimeQueue type that is a queue of Messages.
//Each Message contains a time.Time that describes the time at which the Message should be released from the queue.
//Message types also have a Data field of type interface{} that should be used as the payload of the Message.
//
//Messages need only be pushed to the queue, and then when their time.Time passes (via time.After()),
//they will be sent on the channel returned by Messages(). See below for examples.
//
//Messages that are "released" (sent on the Messages() channel) are always released from a newly spawned go-routine
//so that the main, running routine does not hang and all messages are sent asynchronously to the Messages() channel.
//Messages with the same time.Time value will be "flood-released" at the same time from the separately spawned go-routine.
//
//The TimeQueue uses a single go-routine to receive the time the earliest Message is supposed to be released (via time.After()).
//Whenever a Message has been released, the next earliest Message is then used to determine the next time to wake
//and release the Message.
//Messages that are pushed with earlier times than the earliest already in the queue will be pushed to the front of the queue
//and released appropriately.
//Messages that are pushed with times before time.Now() will immediately be released from the the queue.
package timequeue
