package push

import (
	"encoding/json"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

const (
	occupancy = "[meta]occupancy"
	update    = "update"
	errorType = "error"
)

// NotificationParser struct
type NotificationParser struct {
	keeper    *Keeper
	processor *Processor
	logger    logging.LoggerInterface
}

type incomingEvent struct {
	id         *string
	timestamp  *int64
	encoding   *string
	data       *string
	name       *string
	clientID   *string
	event      string
	channel    *string
	message    *string
	code       *int64
	statusCode *int64
	href       *string
}

// NewNotificationParser creates notifcation parser
func NewNotificationParser(processor *Processor, keeper *Keeper, logger logging.LoggerInterface) *NotificationParser {
	return &NotificationParser{
		keeper:    keeper,
		processor: processor,
		logger:    logger,
	}
}

func (n *NotificationParser) getInt64(data interface{}) *int64 {
	if data == nil {
		return nil
	}

	number, ok := data.(int64)
	if !ok {
		return nil
	}
	return &number
}

func (n *NotificationParser) getString(data interface{}) *string {
	if data == nil {
		return nil
	}

	str, ok := data.(string)
	if !ok {
		return nil
	}
	return &str
}

func (n *NotificationParser) wrapOccupancy(incomingEvent incomingEvent) *dtos.Occupancy {
	if incomingEvent.data == nil {
		return nil
	}

	var occupancy *dtos.Occupancy
	err := json.Unmarshal([]byte(*incomingEvent.data), &occupancy)
	if err != nil {
		return nil
	}

	return occupancy
}

func (n *NotificationParser) wrapUpdateEvent(incomingEvent incomingEvent) *dtos.IncomingNotification {
	if incomingEvent.data == nil {
		return nil
	}
	var incomingNotification *dtos.IncomingNotification
	err := json.Unmarshal([]byte(*incomingEvent.data), &incomingNotification)
	if err != nil {
		n.logger.Error("cannot parse data as IncomingNotification type")
		return nil
	}
	incomingNotification.Channel = *incomingEvent.channel
	return incomingNotification
}

func (n *NotificationParser) wrapIncomingEvent(event map[string]interface{}) incomingEvent {
	incomingEvent := incomingEvent{
		id:         n.getString(event["id"]),
		timestamp:  n.getInt64(event["timestamp"]),
		encoding:   n.getString(event["encoding"]),
		data:       n.getString((event["data"])),
		name:       n.getString(event["name"]),
		clientID:   n.getString(event["clientId"]),
		channel:    n.getString(event["channel"]),
		message:    n.getString(event["message"]),
		code:       n.getInt64(event["code"]),
		statusCode: n.getInt64(event["statusCode"]),
		href:       n.getString(event["href"]),
	}

	if incomingEvent.code != nil && incomingEvent.statusCode != nil {
		incomingEvent.event = errorType
	}

	if incomingEvent.name != nil && *incomingEvent.name == occupancy {
		incomingEvent.event = occupancy
		return incomingEvent
	}

	incomingEvent.event = update
	return incomingEvent
}

// HandleIncomingMessage handles incoming message from streaming
func (n *NotificationParser) HandleIncomingMessage(event map[string]interface{}) {
	incomingEvent := n.wrapIncomingEvent(event)
	switch incomingEvent.event {
	case occupancy:
		n.logger.Debug("Presence event received")
		occupancy := n.wrapOccupancy(incomingEvent)
		if occupancy == nil || incomingEvent.channel == nil {
			return
		}
		n.keeper.UpdateManagers(*incomingEvent.channel, occupancy.Data.Publishers)
		return
	case update:
		n.logger.Debug("Update event received")
		incomingNotification := n.wrapUpdateEvent(incomingEvent)
		if incomingNotification == nil {
			return
		}
		n.logger.Debug("Incoming Notification:", incomingNotification)
		err := n.processor.Process(*incomingNotification)
		if err != nil {
			n.logger.Error(err)
			return
		}
	case errorType:
	default:
		n.logger.Error("Unexpected type of event received")
	}
}
