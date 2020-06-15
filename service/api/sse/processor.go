package sse

import (
	"encoding/json"
	"fmt"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

/*
{
   "id":"St40RHV9u9:0:0",
   "clientId":"pri:NTIxMjUxMjI0",
   "timestamp":1591988399435,
   "encoding":"json",
   "channel":"NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments",
   "data":"{\"type\":\"SEGMENT_UPDATE\",\"changeNumber\":1591988398533,\"segmentName\":\"PUSH_SEGMENT_CSV_MULTIPLE\"}"
}
{
   "id":"gFp3nSE582:0:0",
   "clientId":"pri:MzIxMDYyOTg5MA==",
   "timestamp":1591996685999,
   "encoding":"json",
   "channel":"NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
   "data":"{\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1591996685190}"
}
{
   "id":"ZlalwoKlXW:0:0",
   "clientId":"pri:MzIxMDYyOTg5MA==",
   "timestamp":1591996755043,
   "encoding":"json",
   "channel":"NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits",
   "data":"{\"type\":\"SPLIT_KILL\",\"changeNumber\":1591996754396,\"defaultTreatment\":\"INITIALIZATION_STEP\",\"splitName\":\"PUSH_TEST_2\"}"
}
*/

// Processor struct for notification processor
type Processor struct {
	logger logging.LoggerInterface
}

// NewProcessor creates new processor
func NewProcessor(logger logging.LoggerInterface) *Processor {
	return &Processor{
		logger: logger,
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
}

// Process takes an incoming notification and generates appropriate notifications for it.
func (p *Processor) Process(i dtos.IncomingNotification) error {
	switch i.Type {
	case dtos.SplitUpdate:
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		fmt.Println(splitUpdate)
		// processSplitUpdate
	case dtos.SegmentUpdate:
		segmentUpdate := dtos.NewSegmentChangeNotification(i.Channel, *i.ChangeNumber, *i.SegmentName)
		fmt.Println(segmentUpdate)
		// processSegmentUpdate
	case dtos.SplitKill:
		splitKill := dtos.NewSplitKillNotification(i.Channel, *i.ChangeNumber, *i.DefaultTreatment, *i.SplitName)
		fmt.Println(splitKill)
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
