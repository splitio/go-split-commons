package queue

import (
	"testing"

	"github.com/splitio/push-notification-manager/splitio/notifications"
)

func TestSingleQueue(t *testing.T) {
	segmentsUpdatesQueue := NewQueue(notifications.SegmentUpdate, 5000)
	err := segmentsUpdatesQueue.Put(notifications.NewSplitChangeNotification("chan2", 123456789))
	if err == nil || err.Error() != "Message was not be inserted for wrong type. Expecting SEGMENT_UPDATE" {
		t.Error("It should return error")
	}

	for i := 0; i < 10; i++ {
		_ = segmentsUpdatesQueue.Put(notifications.NewSegmentChangeNotification("chan1", 123456789, "mySegment"))
	}

	if len(segmentsUpdatesQueue.Pop(15)) != 10 {
		t.Error("It should got 10 items")
	}

	noNameQueue := NewQueue("NO_NAME", 5000)
	for i := 0; i < 10; i++ {
		_ = noNameQueue.Put(notifications.NewSegmentChangeNotification("chan1", 123456789, "mySegment"))
	}

	if len(noNameQueue.Pop(15)) != 0 {
		t.Error("It should not added items")
	}
}

func TestQueueSizeWorks(t *testing.T) {
	queue := NewQueue(notifications.SplitUpdate, 10)
	queue.Put(notifications.NewSplitChangeNotification("someChannel", 0))
	if queue.Size() != 1 {
		t.Error("Queue size should be 1. Is: ", queue.Size())
	}

	for i := 1; i < 10; i++ {
		queue.Put(notifications.NewSplitChangeNotification("someChannel", int64(i)))
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
