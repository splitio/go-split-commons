package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	segmentQueueCheck = 5000
	splitQueueCheck   = 5000
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
	if cap(segmentQueue) < segmentQueueCheck {
		return nil, errors.New("Small size of segmentQueue")
	}
	if cap(splitQueue) < splitQueueCheck {
		return nil, errors.New("Small size of splitQueue")
	}

	return &Processor{
		segmentQueue: segmentQueue,
		splitQueue:   splitQueue,
		splitStorage: splitStorage,
		logger:       logger,
	}, nil
}

// Process takes an incoming notification and generates appropriate notifications for it.
func (p *Processor) Process(i dtos.IncomingNotification) error {
	switch i.Type {
	case dtos.SplitUpdate:
		if i.ChangeNumber == nil {
			return errors.New("ChangeNumber could not be nil, discarded")
		}
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		p.splitQueue <- splitUpdate
	case dtos.SegmentUpdate:
		if i.ChangeNumber == nil {
			return errors.New("ChangeNumber could not be nil, discarded")
		}
		if i.SegmentName == nil {
			return errors.New("SegmentName could not be nil, discarded")
		}
		segmentUpdate := dtos.NewSegmentChangeNotification(i.Channel, *i.ChangeNumber, *i.SegmentName)
		p.segmentQueue <- segmentUpdate
	case dtos.SplitKill:
		if i.ChangeNumber == nil {
			return errors.New("ChangeNumber could not be nil, discarded")
		}
		if i.SplitName == nil {
			return errors.New("SplitName could not be nil, discarded")
		}
		if i.DefaultTreatment == nil {
			return errors.New("DefaultTreatment could not be nil, discarded")
		}
		splitUpdate := dtos.NewSplitChangeNotification(i.Channel, *i.ChangeNumber)
		p.splitStorage.KillLocally(*i.SplitName, *i.DefaultTreatment)
		p.splitQueue <- splitUpdate
	case dtos.Control:
		if i.ControlType == nil {
			return errors.New("ControlType could not be nil, discarded")
		}
		control := dtos.NewControlNotification(i.Channel, *i.ControlType)
		fmt.Println(control)
		// processControl
	default:
		return fmt.Errorf("Unknown IncomingNotification type: %T", i)
	}
	return nil
}
