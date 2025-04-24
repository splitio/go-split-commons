package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/v7/conf"
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	splitQueueMinSize        = 5000
	segmentQueueMinSize      = 5000
	largeSegmentQueueMinSize = 5000
)

type LargeSegment struct {
	queue  chan dtos.LargeSegmentChangeUpdate
	worker *LargeSegmentUpdateWorker
}

// Processor provides the interface for an update-message processor
type Processor interface {
	ProcessSplitChangeUpdate(update *dtos.SplitChangeUpdate) error
	ProcessSplitKillUpdate(update *dtos.SplitKillUpdate) error
	ProcessSegmentChangeUpdate(update *dtos.SegmentChangeUpdate) error
	ProcessLargeSegmentChangeUpdate(update *dtos.LargeSegmentChangeUpdate) error
	StartWorkers()
	StopWorkers()
}

// ProcessorImpl struct for notification processor
type ProcessorImpl struct {
	segmentQueue  chan dtos.SegmentChangeUpdate
	splitQueue    chan dtos.SplitChangeUpdate
	splitWorker   *SplitUpdateWorker
	segmentWorker *SegmentUpdateWorker
	synchronizer  synchronizerInterface
	logger        logging.LoggerInterface
	largeSegment  *LargeSegment
}

// NewProcessor creates new processor
func NewProcessor(
	splitQueueSize int64,
	segmentQueueSize int64,
	synchronizer synchronizerInterface,
	logger logging.LoggerInterface,
	lscfg *conf.LargeSegmentConfig,
) (*ProcessorImpl, error) {
	if segmentQueueSize < segmentQueueMinSize {
		return nil, errors.New("small size of segmentQueue")
	}
	if splitQueueSize < splitQueueMinSize {
		return nil, errors.New("small size of splitQueue")
	}

	splitQueue := make(chan dtos.SplitChangeUpdate, splitQueueSize)
	splitWorker, err := NewSplitUpdateWorker(splitQueue, synchronizer, logger)
	if err != nil {
		return nil, fmt.Errorf("error instantiating split worker: %w", err)
	}

	segmentQueue := make(chan dtos.SegmentChangeUpdate, segmentQueueSize)
	segmentWorker, err := NewSegmentUpdateWorker(segmentQueue, synchronizer, logger)
	if err != nil {
		return nil, fmt.Errorf("error instantiating split worker: %w", err)
	}

	var largeSegment *LargeSegment
	if lscfg != nil && lscfg.Enable {
		if lscfg.UpdateQueueSize < largeSegmentQueueMinSize {
			return nil, errors.New("small size of largeSegmentQueueSize")
		}
		lsUpdateQueue := make(chan dtos.LargeSegmentChangeUpdate, lscfg.UpdateQueueSize)
		lsWorker, err := NewLargeSegmentUpdateWorker(lsUpdateQueue, synchronizer, logger)
		if err != nil {
			return nil, fmt.Errorf("error instantiating large segment worker: %w", err)
		}

		largeSegment = &LargeSegment{
			queue:  lsUpdateQueue,
			worker: lsWorker,
		}
	}

	return &ProcessorImpl{
		splitQueue:    splitQueue,
		splitWorker:   splitWorker,
		segmentQueue:  segmentQueue,
		segmentWorker: segmentWorker,
		synchronizer:  synchronizer,
		logger:        logger,
		largeSegment:  largeSegment,
	}, nil
}

// ProcessSplitChangeUpdate accepts a split change notifications and schedules a fetch
func (p *ProcessorImpl) ProcessSplitChangeUpdate(update *dtos.SplitChangeUpdate) error {
	if update == nil {
		return errors.New("split change update cannot be nil")
	}
	p.splitQueue <- *update
	return nil
}

// ProcessSplitKillUpdate accepts a split kill notification, issues a local kill and schedules a fetch
func (p *ProcessorImpl) ProcessSplitKillUpdate(update *dtos.SplitKillUpdate) error {
	if update == nil {
		return errors.New("split change update cannot be nil")
	}
	p.synchronizer.LocalKill(update.SplitName(), update.DefaultTreatment(), update.ChangeNumber())
	return p.ProcessSplitChangeUpdate(update.ToSplitChangeUpdate())
}

// ProcessSegmentChangeUpdate accepts a segment change notification and schedules a fetch
func (p *ProcessorImpl) ProcessSegmentChangeUpdate(update *dtos.SegmentChangeUpdate) error {
	if update == nil {
		return errors.New("split change update cannot be nil")
	}
	p.segmentQueue <- *update
	return nil
}

func (p *ProcessorImpl) ProcessLargeSegmentChangeUpdate(update *dtos.LargeSegmentChangeUpdate) error {
	if p.largeSegment == nil {
		return nil
	}

	if update == nil {
		return errors.New("large segment change update cannot be nil")
	}
	p.largeSegment.queue <- *update
	return nil
}

// StartWorkers enables split & segments workers
func (p *ProcessorImpl) StartWorkers() {
	p.splitWorker.Start()
	p.segmentWorker.Start()
	if p.largeSegment != nil {
		p.largeSegment.worker.Start()
	}
}

// StopWorkers pauses split & segments workers
func (p *ProcessorImpl) StopWorkers() {
	p.splitWorker.Stop()
	p.segmentWorker.Stop()
	if p.largeSegment != nil {
		p.largeSegment.worker.Stop()
	}
}
