package processor

import (
	"encoding/json"
	"fmt"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/queue"
	"github.com/splitio/go-toolkit/logging"
)

// Processor struct for notification processor
type Processor struct {
	segmentQueue queue.Queue
	splitQueue   queue.Queue
	logger       logging.LoggerInterface
}

// NewProcessor creates new processor
func NewProcessor(logger logging.LoggerInterface) *Processor {
	return &Processor{
		segmentQueue: queue.NewQueue(dtos.SegmentUpdate, 5000),
		splitQueue:   queue.NewQueue(dtos.SplitUpdate, 5000),
		logger:       logger,
	}
}

// HandleIncomingMessage handles incoming message from streaming
func (p *Processor) HandleIncomingMessage(event map[string]interface{}) {
	if event["data"] == nil {
		p.logger.Error("data is not present in incoming notification")
		return
	}
	strEvent, ok := event["data"].(string)
	if !ok {
		p.logger.Error("wrong type of data")
		return
	}

	var incomingNotification dtos.IncomingNotification
	err := json.Unmarshal([]byte(strEvent), &incomingNotification)
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
	err = p.Process(incomingNotification)
	if err != nil {
		p.logger.Error(err)
	}
}

// Process takes an incoming notification and generates appropriate notifications for it.
func (p *Processor) Process(i dtos.IncomingNotification) error {
	switch i.Type {
	case dtos.SplitUpdate:
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		p.splitQueue.Put(splitUpdate)
		// processSplitUpdate
	case dtos.SegmentUpdate:
		segmentUpdate := dtos.NewSegmentChangeNotification(i.Channel, *i.ChangeNumber, *i.SegmentName)
		p.segmentQueue.Put(segmentUpdate)
		// processSegmentUpdate
	case dtos.SplitKill:
		// splitKill := dtos.NewSplitKillNotification(i.Channel, *i.ChangeNumber, *i.DefaultTreatment, *i.SplitName)
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		p.splitQueue.Put(splitUpdate)
		// processSplitKill
	case dtos.Control:
		control := dtos.NewControlNotification(i.Channel, *i.ControlType)
		fmt.Println(control)
		// processControl
	default:
		return fmt.Errorf("Unknown IncomingNotification type: %T", i)
	}
	return nil
}
