//Package timequeue provides the TimeQueue type that is a queue of Messages.
//Each Message contains a time.Time that describes the time at which the Message should be released from the queue.
//Message types also have a Data field of type interface{} that should be used as the payload of the Message.
//
//Messages need only be pushed to the queue, and then when their time.Time passes (via time.After()),
//they will be sent on the channel returned by Messages(). See below for examples.
//
//The TimeQueue uses a single go-routine to receive the time the earliest Message is supposed to be released (via time.After()).
//Whenever a Message has been released, the next earliest Message is then used to determine the next time to wake
//and release the Message.
//Messages that are released are always released from a newly spawned go-routine so that the main, running routine does
//not hang and all messages are sent asynchronously to Messages() channel.
//Messages with the same time.Time value will be "flood-released" at the same time from the same, separate go-routine.
package timequeue
