package timequeue

import (
	"fmt"
	"time"
)

type Message struct {
	time.Time
	Data interface{}
}

func (m *Message) String() string {
	return fmt.Sprintf("%#v", m)
}

type messageHeap []*Message

func (mh messageHeap) Len() int {
	return len(mh)
}

func (mh messageHeap) Less(i, j int) bool {
	return mh[i].Time.Sub(mh[j].Time) < 0
}

func (mh messageHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
}

func (mh *messageHeap) Push(value interface{}) {
	*mh = append(*mh, value.(*Message))
}

func (mh *messageHeap) Pop() interface{} {
	n := len(*mh)
	result := (*mh)[n-1]
	*mh = (*mh)[0 : n-1]
	return result
}
