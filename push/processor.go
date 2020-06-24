package push

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	keepalive         = "keepalive"
	occupancy         = "[meta]occupancy"
	update            = "update"
	errorType         = "error"
	segmentQueueCheck = 5000
	splitQueueCheck   = 5000
)

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

// Processor struct for notification processor
type Processor struct {
	segmentQueue chan dtos.SegmentChangeNotification
	splitQueue   chan dtos.SplitChangeNotification
	splitStorage storage.SplitStorageProducer
	keepAlive    chan struct{}
	keeper       Keeper
	logger       logging.LoggerInterface
}

// NewProcessor creates new processor
func NewProcessor(segmentQueue chan dtos.SegmentChangeNotification, splitQueue chan dtos.SplitChangeNotification, splitStorage storage.SplitStorageProducer,
	keepAlive chan struct{}, keeper Keeper, logger logging.LoggerInterface) (*Processor, error) {
	if cap(segmentQueue) < segmentQueueCheck {
		return nil, errors.New("Small size of segmentQueue")
	}
	if cap(splitQueue) < splitQueueCheck {
		return nil, errors.New("Small size of splitQueue")
	}
	if cap(keepAlive) < 1 {
		return nil, errors.New("KeepAlive handler should have capacity")
	}

	return &Processor{
		segmentQueue: segmentQueue,
		splitQueue:   splitQueue,
		splitStorage: splitStorage,
		keepAlive:    keepAlive,
		keeper:       keeper,
		logger:       logger,
	}, nil
}

func (p *Processor) getInt64(data interface{}) *int64 {
	if data == nil {
		return nil
	}

	number, ok := data.(int64)
	if !ok {
		return nil
	}
	return &number
}

func (p *Processor) getString(data interface{}) *string {
	if data == nil {
		return nil
	}

	str, ok := data.(string)
	if !ok {
		return nil
	}
	return &str
}

func (p *Processor) wrapUpdateEvent(incomingEvent incomingEvent) *dtos.IncomingNotification {
	if incomingEvent.data == nil {
		return nil
	}
	var incomingNotification *dtos.IncomingNotification
	err := json.Unmarshal([]byte(*incomingEvent.data), &incomingNotification)
	if err != nil {
		p.logger.Error("cannot parse data as IncomingNotification type")
		return nil
	}
	incomingNotification.Channel = *incomingEvent.channel
	return incomingNotification
}

func (p *Processor) wrapOccupancy(data *string) *dtos.Occupancy {
	fmt.Println("data", *data)
	if data == nil {
		return nil
	}
	var occupancy *dtos.Occupancy
	err := json.Unmarshal([]byte(*data), &occupancy)
	if err != nil {
		return nil
	}

	return occupancy
}

func (p *Processor) wrapIncomingEvent(event map[string]interface{}) incomingEvent {
	incomingEvent := incomingEvent{
		id:         p.getString(event["id"]),
		timestamp:  p.getInt64(event["timestamp"]),
		encoding:   p.getString(event["encoding"]),
		data:       p.getString((event["data"])),
		name:       p.getString(event["name"]),
		clientID:   p.getString(event["clientId"]),
		channel:    p.getString(event["channel"]),
		message:    p.getString(event["message"]),
		code:       p.getInt64(event["code"]),
		statusCode: p.getInt64(event["statusCode"]),
		href:       p.getString(event["href"]),
	}

	eventType := p.getString(event["event"])
	if eventType != nil && *eventType == keepalive {
		incomingEvent.event = keepalive
		return incomingEvent
	}

	if incomingEvent.name != nil && *incomingEvent.name == occupancy {
		incomingEvent.event = occupancy
		return incomingEvent
	}

	if incomingEvent.code != nil && incomingEvent.statusCode != nil {
		incomingEvent.event = errorType
	}

	incomingEvent.event = update
	return incomingEvent
}

// HandleIncomingMessage handles incoming message from streaming
func (p *Processor) HandleIncomingMessage(event map[string]interface{}) {
	// p.logger.Info("EVENT: ", event)
	incomingEvent := p.wrapIncomingEvent(event)
	switch incomingEvent.event {
	case keepalive:
		p.logger.Debug("KeepAlive event received")
		p.keepAlive <- struct{}{}
		return
	case occupancy:
		p.logger.Debug("Presence event received")
		occupancy := p.wrapOccupancy(incomingEvent.data)
		if occupancy == nil || incomingEvent.channel == nil {
			return
		}
		p.keeper.UpdateManagers(*incomingEvent.channel, occupancy.Data.Publishers)
		return
	case update:
		p.logger.Debug("Update event received")
		incomingNotification := p.wrapUpdateEvent(incomingEvent)
		if incomingNotification == nil {
			return
		}
		p.logger.Debug("Incoming Notification:", incomingNotification)
		err := p.process(incomingNotification)
		if err != nil {
			p.logger.Error(err)
			return
		}
		if incomingEvent.channel != nil && incomingEvent.timestamp != nil {
			p.keeper.UpdateLastNotification(*incomingEvent.channel, *incomingEvent.timestamp)
		}
	case errorType:
	default:
		p.logger.Error("Unexpected type of event received")
	}
}

// Process takes an incoming notification and generates appropriate notifications for it.
func (p *Processor) process(i *dtos.IncomingNotification) error {
	switch i.Type {
	case dtos.SplitUpdate:
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		p.splitQueue <- splitUpdate
	case dtos.SegmentUpdate:
		segmentUpdate := dtos.NewSegmentChangeNotification(i.Channel, *i.ChangeNumber, *i.SegmentName)
		p.segmentQueue <- segmentUpdate
	case dtos.SplitKill:
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		p.splitStorage.KillLocally(*i.SplitName, *i.DefaultTreatment)
		p.splitQueue <- splitUpdate
	case dtos.Control:
		control := dtos.NewControlNotification(i.Channel, *i.ControlType)
		fmt.Println(control)
		// processControl
	default:
		return fmt.Errorf("Unknown IncomingNotification type: %T", i)
	}
	return nil
}
