package timequeue

import (
	"container/heap"
	"math/rand"
	"reflect"
	"sort"
	"time"
	"testing"
)

var (
	_ heap.Interface = new(messageHeap)
)

func TestNewMessage(t *testing.T) {
	now := time.Now()
	p := Priority(1234)
	var data interface{} = t.Name()

	m := NewMessage(now, p, data)

	if !m.At.Equal(now) {
		t.Fatal("At")
	}
	if m.Priority != p {
		t.Fatal("Priority")
	}
	if !reflect.DeepEqual(m.Data, data) {
		t.Fatal("Data")
	}

	if m.messageHeap != nil {
		t.Fatal("messageHeap")
	}
	if m.index != indexNotInHeap {
		t.Fatal("index")
	}
}

func TestMessage_less(t *testing.T) {
	now := time.Now()

	cases := []struct{
		a Message
		b Message
		result bool
	}{
		{
			Message{At: now},
			Message{At: now.Add(-1)},
			false,
		},
		{
			Message{At: now.Add(-1)},
			Message{At: now},
			true,
		},
		{
			Message{At: now},
			Message{At: now},
			false,
		},
		{
			Message{At: now, Priority: 1},
			Message{At: now.Add(-1), Priority: 2},
			false,
		},
		{
			Message{At: now.Add(-1), Priority: 2},
			Message{At: now, Priority: 1},
			true,
		},
		{
			Message{At: now, Priority: 1},
			Message{At: now, Priority: 2},
			true,
		},
		{
			Message{At: now, Priority: 2},
			Message{At: now, Priority: 2},
			false,
		},
		{
			Message{At: now, Priority: 3},
			Message{At: now, Priority: 2},
			false,
		},
	}

	for i, tc := range cases {
		result := tc.a.less(&tc.b)

		if result != tc.result {
			t.Errorf("%d: %v less %v = %v WANT %v", i, tc.a, tc.b, result, tc.result)
		}
	}
}

func TestMessage_isHead_NewMessagesShouldNotBeHeads(t *testing.T) {
	m := NewMessage(time.Now(), 0, nil)
	if m.isHead() {
		t.Fatal()
	}
}

func TestMessage_isHead_MessagesInLenOneHeapsAreHeads(t *testing.T) {
	mh := messageHeap([]*Message{})
	m := NewMessage(time.Now(), 0, nil)

	pushMessage(&mh, &m)

	if !m.isHead() {
		t.Fatal()
	}
}

func TestMessageHeap_Len(t *testing.T) {
	mh := messageHeap([]*Message{})
	if mh.Len() != 0 {
		t.Fatal()
	}

	mh = messageHeap(make([]*Message, 1234))
	if mh.Len() != 1234 {
		t.Fatal()
	}
}

func TestMessageHeap_Less_DefersToTheMessageLessMethod(t *testing.T) {
	now := time.Now()
	m1 := NewMessage(now, 0, nil)
	m2 := NewMessage(now, 1, nil)

	mh := messageHeap([]*Message{&m1, &m2})

	if !mh.Less(0, 1) {
		t.Fatal()
	}
	if mh.Less(1, 0) {
		t.Fatal()
	}
}

func TestMessageHeap_Swap_UpdatesReferencesAndIndices(t *testing.T) {
	now := time.Now()
	m1 := NewMessage(now, 0, nil)
	m2 := NewMessage(now, 1, nil)

	mh := messageHeap([]*Message{&m1, &m2})

	mh.Swap(0, 1)

	//Messages weren't pushed, so there isn't information on them.
	//We can check to make sure the index is updated.

	if mh[0] != &m2 || m2.index != 0 {
		t.Fatal()
	}
	if mh[1] != &m1 || m1.index != 1 {
		t.Fatal()
	}
}

func TestMessageHeap_Push_SetsTheMessageHeapFieldOnMessage(t *testing.T) {
	m := NewMessage(time.Now(), 0, nil)

	mh := messageHeap([]*Message{})

	pushMessage(&mh, &m)

	if m.messageHeap != &mh {
		t.Fatal()
	}
}

func TestMessageHeap_PushAndPopResultInTheCorrectOrdering(t *testing.T) {
	now := time.Now()

	mh := messageHeap([]*Message{})

	want := []*Message{}
	for i := 0; i < 100; i++ {
		m := NewMessage(now, Priority(rand.Int31()), nil)
		want = append(want, &m)

		pushMessage(&mh, &m)
	}
	sort.Sort(messageHeap(want))

	result := []*Message{}
	for mh.Len() > 0 {
		result = append(result, popMessage(&mh))
	}

	//Do a loop here to check equality of pointer values.
	for i, m := range result {
		if m != want[i] {
			t.Fatal()
		}
	}
}

func TestMessageHeap_peek_EmptyReturnsNil(t *testing.T) {
	mh := messageHeap([]*Message{})

	if r := mh.peek(); r != nil {
		t.Fatal()
	}
}

func TestMessageHeap_peek_ReturnsMessageAtIndexZero(t *testing.T) {
	m := NewMessage(time.Now(), 0, nil)

	mh := messageHeap([]*Message{})

	pushMessage(&mh, &m)

	if peeked := mh.peek(); peeked != mh[0] {
		t.Fatal()
	}
}

func TestMessageHeap_remove_ReturnsFalseWithoutAssociation(t *testing.T) {
	m := NewMessage(time.Now(), 0, nil)

	mh := messageHeap([]*Message{})

	if ok := mh.remove(&m); ok {
		t.Fatal()
	}
}

func TestMessageHeap_remove_ReturnsTrueAndModifiesMessage(t *testing.T) {
	m := NewMessage(time.Now(), 0, nil)

	mh := messageHeap([]*Message{})

	pushMessage(&mh, &m)

	if m.index < 0 {
		t.Fatal()
	}

	if ok := mh.remove(&m); !ok {
		t.Fatal()
	}

	if m.index >= 0 {
		t.Fatal()
	}
}
