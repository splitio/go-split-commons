package synchronizer

import (
	"fmt"
	"sync"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
)

// SegmentSynchronizer struct for segment sync
type SegmentSynchronizer struct {
	splitStorage   storage.SplitStorage
	segmentStorage storage.SegmentStorage
	segmentFetcher service.SegmentFetcher
	logger         logging.LoggerInterface
}

// NewSegmentSynchronizer creates new segment synchronizer for processing segment updates
func NewSegmentSynchronizer(
	splitStorage storage.SplitStorage,
	segmentStorage storage.SegmentStorage,
	segmentFetcher service.SegmentFetcher,
	logger logging.LoggerInterface,
) *SegmentSynchronizer {
	return &SegmentSynchronizer{
		splitStorage:   splitStorage,
		segmentStorage: segmentStorage,
		segmentFetcher: segmentFetcher,
		logger:         logger,
	}
}

func (s *SegmentSynchronizer) processUpdate(segmentChanges *dtos.SegmentChangesDTO) {
	name := segmentChanges.Name
	oldSegment := s.segmentStorage.Get(name)
	if oldSegment == nil {
		keys := set.NewSet()
		for _, key := range segmentChanges.Added {
			keys.Add(key)
		}
		s.segmentStorage.Put(name, keys, segmentChanges.Till)
	} else {
		// Segment exists, must add new members and remove old ones
		for _, key := range segmentChanges.Added {
			oldSegment.Add(key)
		}
		for _, key := range segmentChanges.Removed {
			oldSegment.Remove(key)
		}
		s.segmentStorage.Put(name, oldSegment, segmentChanges.Till)
	}
}

// SynchronizeSegment syncs segment
func (s *SegmentSynchronizer) SynchronizeSegment(name string, till *int64) error {
	for {
		changeNumber, _ := s.segmentStorage.ChangeNumber(name)
		if changeNumber == 0 {
			changeNumber = -1
		}

		segmentChanges, err := s.segmentFetcher.Fetch(name, changeNumber)
		if err != nil {
			return err
		}

		s.processUpdate(segmentChanges)

		if segmentChanges.Since == segmentChanges.Till {
			return nil
		}
	}
}

// SynchronizeSegments syncs segments at once
func (s *SegmentSynchronizer) SynchronizeSegments() error {
	// @TODO: add delays
	segmentNames := s.splitStorage.SegmentNames().List()
	s.logger.Debug("Segment Sync", segmentNames)
	wg := sync.WaitGroup{}
	wg.Add(len(segmentNames))
	failedSegments := make([]string, 0)
	for _, name := range segmentNames {
		conv, ok := name.(string)
		if !ok {
			s.logger.Warning("Skipping non-string segment present in storage at initialization-time!")
			continue
		}
		go func(segmentName string) {
			defer wg.Done() // Make sure the "finished" signal is always sent
			ready := false
			var err error
			for !ready {
				err = s.SynchronizeSegment(segmentName, nil)
				if err != nil {
					failedSegments = append(failedSegments, segmentName)
					return
				}
			}
		}(conv)
	}
	wg.Wait()

	if len(failedSegments) > 0 {
		return fmt.Errorf("The following segments failed to be fetched %v", failedSegments)
	}

	return nil
}

// SegmentNames returns all segments
func (s *SegmentSynchronizer) SegmentNames() []interface{} {
	return s.splitStorage.SegmentNames().List()
}
