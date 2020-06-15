package queue

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
)

func TestSingleQueue(t *testing.T) {
	segmentsUpdatesQueue := NewQueue(dtos.SegmentUpdate, 5000)
	err := segmentsUpdatesQueue.Put(dtos.NewSplitChangeNotification("chan2", 123456789))
	if err == nil || err.Error() != "Message was not be inserted for wrong type. Expecting SEGMENT_UPDATE" {
		t.Error("It should return error")
	}

	for i := 0; i < 10; i++ {
		_ = segmentsUpdatesQueue.Put(dtos.NewSegmentChangeNotification("chan1", 123456789, "mySegment"))
	}

	if len(segmentsUpdatesQueue.Pop(15)) != 10 {
		t.Error("It should got 10 items")
	}

	noNameQueue := NewQueue("NO_NAME", 5000)
	for i := 0; i < 10; i++ {
		_ = noNameQueue.Put(dtos.NewSegmentChangeNotification("chan1", 123456789, "mySegment"))
	}

	if len(noNameQueue.Pop(15)) != 0 {
		t.Error("It should not added items")
	}
}

func TestQueueSizeWorks(t *testing.T) {
	queue := NewQueue(dtos.SplitUpdate, 10)
	queue.Put(dtos.NewSplitChangeNotification("someChannel", 0))
	if queue.Size() != 1 {
		t.Error("Queue size should be 1. Is: ", queue.Size())
	}

	for i := 1; i < 10; i++ {
		queue.Put(dtos.NewSplitChangeNotification("someChannel", int64(i)))
	}
	if queue.Size() != 10 {
		t.Error("Queue size should be 10. Is: ", queue.Size())
	}

	queue.Pop(5)
	if queue.Size() != 5 {
		t.Error("Queue size should be 5. Is: ", queue.Size())
	}

	queue.Pop(5)
	if queue.Size() != 0 {
		t.Error("Queue size should be 0. Is: ", queue.Size())
	}
}
