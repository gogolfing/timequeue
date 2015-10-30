//Package timequeue provides the TimeQueue type that is a queue of Messages.
//Each Message contains a time.Time that describes the time at which the Message
//should be released from the queue.
//Message types also have a Data field of type interface{} that should be used
//as the payload of the Message.
//TimeQueue is safe for use by multiple go-routines.
//
//Messages need only be pushed to the queue, and then when their time passes,
//they will be sent on the channel returned by Messages().
//See below for examples.

//TimeQueue uses a single go-routine, spawned from Start() that returns from Stop(),
//that processes the Messages as their times pass.
//When a Message is pushed to the queue, the earliest Message in the queue is
//used to determine the next time the running go-routine should wake.
//The running go-routine knows when to wake because the earliest time is used
//to make a channel via time.After(). Receiving on that channel wakes the
//running go-routine, is a call to Stop() does not happen prior.
//Upon waking, that Message is removed from the queue and released on the channel
//returned from Messages().
//Then the newest remaining Message is used to determine when to wake, etc.
//If a Message with a time before any other in the queue are inserted, then that
//Message is pushed to the front of the queue and released appropriately.
//
//Message that are "released", i.e. send on the Messages() channel, are always
//released from a newly spawned go-routine so that other go-routines are not
//paused waiting for a receive from Messages().
//
//Messages with the same Time value will be "flood-released" from the same
//separately spawned go-routine.
//Additionally, Messages that are pushed with times before time.Now() will
//immediately be released from the queue.
package timequeue
