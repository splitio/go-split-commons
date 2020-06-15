package queue

import (
	"fmt"

	"github.com/splitio/go-split-commons/dtos"
)

// Queue represents each queue type
type Queue struct {
	name  string
	queue chan dtos.Notification
}

// NewQueue creates new queue
func NewQueue(name string, size int64) Queue {
	return Queue{
		name:  name,
		queue: make(chan dtos.Notification, size),
	}
}

// Put adds message into queue
func (n *Queue) Put(message dtos.Notification) error {
	if n.name != message.NotificationType() {
		return fmt.Errorf("Message was not be inserted for wrong type. Expecting %s", n.name)
	}
	n.queue <- message
	return nil
}

// Pop gets m elements from queue
func (n *Queue) Pop(m int) []dtos.Notification {
	messages := make([]dtos.Notification, 0, m)
	for i := 0; i < m; i++ {
		select {
		case item := <-n.queue:
			messages = append(messages, item)
		default:
			break
		}
	}
	return messages
}

// Size returns queue's size
func (n *Queue) Size() int {
	return len(n.queue)
}
