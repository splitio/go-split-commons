package push

import (
	"encoding/json"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

// EventHandler struct
type EventHandler struct {
	parser    *NotificationParser
	processor *Processor
	logger    logging.LoggerInterface
}

// NewEventHandler builds new EventHandler
func NewEventHandler(parser *NotificationParser, processor *Processor, logger logging.LoggerInterface) *EventHandler {
	return &EventHandler{
		parser:    parser,
		processor: processor,
		logger:    logger,
	}
}

func (e *EventHandler) wrapUpdateEvent(incomingEvent IncomingEvent) *dtos.IncomingNotification {
	if incomingEvent.data == nil {
		return nil
	}
	var incomingNotification *dtos.IncomingNotification
	err := json.Unmarshal([]byte(*incomingEvent.data), &incomingNotification)
	if err != nil {
		e.logger.Error("cannot parse data as IncomingNotification type")
		return nil
	}
	incomingNotification.Channel = *incomingEvent.channel
	return incomingNotification
}

// HandleIncomingMessage handles incoming message from streaming
func (e *EventHandler) HandleIncomingMessage(event map[string]interface{}) {
	incomingEvent := e.parser.Parse(event)
	switch incomingEvent.event {
	case update:
		e.logger.Debug("Update event received")
		incomingNotification := e.wrapUpdateEvent(incomingEvent)
		if incomingNotification == nil {
			return
		}
		e.logger.Debug("Incoming Notification:", incomingNotification)
		err := e.processor.Process(*incomingNotification)
		if err != nil {
			return
		}
	case errorType:
	default:
		e.logger.Error("Unexpected type of event received")
	}
}
