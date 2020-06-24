package processor

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	keepalive = "keepalive"
)

// Processor struct for notification processor
type Processor struct {
	segmentQueue chan dtos.SegmentChangeNotification
	splitQueue   chan dtos.SplitChangeNotification
	splitStorage storage.SplitStorageProducer
	logger       logging.LoggerInterface
}

// NewProcessor creates new processor
func NewProcessor(segmentQueue chan dtos.SegmentChangeNotification, splitQueue chan dtos.SplitChangeNotification, splitStorage storage.SplitStorageProducer, logger logging.LoggerInterface) (*Processor, error) {
	if cap(segmentQueue) < 5000 {
		return nil, errors.New("Small size of segmentQueue")
	}
	if cap(splitQueue) < 5000 {
		return nil, errors.New("Small size of splitQueue")
	}

	return &Processor{
		segmentQueue: segmentQueue,
		splitQueue:   splitQueue,
		splitStorage: splitStorage,
		logger:       logger,
	}, nil
}

func (p *Processor) getData(data interface{}) *string {
	if data == nil {
		return nil
	}

	str, ok := data.(string)
	if !ok {
		return nil
	}
	return &str
}

// HandleIncomingMessage handles incoming message from streaming
func (p *Processor) HandleIncomingMessage(event map[string]interface{}) {
	keepAliveEvent := p.getData(event["event"])
	if keepAliveEvent != nil && *keepAliveEvent == keepalive {
		// Reset Timer Connection
		p.logger.Info("RESET TIMER")
		return
	}

	updateEvent := p.getData(event["data"])
	if updateEvent == nil {
		p.logger.Error("data is not present in incoming notification")
		return
	}

	var incomingNotification dtos.IncomingNotification
	err := json.Unmarshal([]byte(*updateEvent), &incomingNotification)
	if err != nil {
		p.logger.Error("cannot parse data as IncomingNotification type")
		return
	}

	if event["channel"] != nil {
		channel, ok := event["channel"].(string)
		if ok {
			incomingNotification.Channel = channel
		}
	}

	p.logger.Debug("Incomming Notification:", incomingNotification)
	err = p.process(incomingNotification)
	if err != nil {
		p.logger.Error(err)
	}
}

// Process takes an incoming notification and generates appropriate notifications for it.
func (p *Processor) process(i dtos.IncomingNotification) error {
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
