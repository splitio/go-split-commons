package push

import (
	"github.com/splitio/go-toolkit/common"
	"github.com/splitio/go-toolkit/logging"
)

const (
	update    = "update"
	errorType = "error"
)

// NotificationParser struct
type NotificationParser struct {
	logger logging.LoggerInterface
}

// NewNotificationParser creates notifcation parser
func NewNotificationParser(logger logging.LoggerInterface) *NotificationParser {
	return &NotificationParser{
		logger: logger,
	}
}

// Parse parses incoming event from streaming
func (n *NotificationParser) Parse(event map[string]interface{}) IncomingEvent {
	incomingEvent := IncomingEvent{
		id:         common.AsStringOrNil(event["id"]),
		timestamp:  common.AsInt64OrNil(event["timestamp"]),
		encoding:   common.AsStringOrNil(event["encoding"]),
		data:       common.AsStringOrNil(event["data"]),
		name:       common.AsStringOrNil(event["name"]),
		clientID:   common.AsStringOrNil(event["clientId"]),
		channel:    common.AsStringOrNil(event["channel"]),
		message:    common.AsStringOrNil(event["message"]),
		code:       common.AsInt64OrNil(event["code"]),
		statusCode: common.AsInt64OrNil(event["statusCode"]),
		href:       common.AsStringOrNil(event["href"]),
	}

	if incomingEvent.code != nil && incomingEvent.statusCode != nil {
		incomingEvent.event = errorType
	} else {
		incomingEvent.event = update
	}
	return incomingEvent
}
