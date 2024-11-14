package synchronizer

func (s *SynchronizerImpl) dataFlusher() {
	for {
		msg := <-s.inMememoryFullQueue
		switch msg {
		case "EVENTS_FULL":
			s.logger.Debug("FLUSHING storage queue")
			err := s.workers.EventRecorder.SynchronizeEvents(s.eventBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
		case "IMPRESSIONS_FULL":
			s.logger.Debug("FLUSHING storage queue")
			err := s.workers.ImpressionRecorder.SynchronizeImpressions(s.impressionBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
		}
	}
}

func (s *SynchronizerImpl) filterCachedSegments(segmentsReferenced []string) []string {
	toRet := make([]string, 0, len(segmentsReferenced))
	for _, name := range segmentsReferenced {
		if !s.workers.SegmentUpdater.IsSegmentCached(name) {
			toRet = append(toRet, name)
		}
	}
	return toRet
}

func (s *SynchronizerImpl) filterCachedLargeSegments(ReferencedLargeSegments []string) []string {
	toRet := make([]string, 0, len(ReferencedLargeSegments))
	for _, name := range ReferencedLargeSegments {
		if !s.workers.LargeSegmentUpdater.IsCached(name) {
			toRet = append(toRet, name)
		}
	}
	return toRet
}

func (s *SynchronizerImpl) synchronizeSegmentsAfterSplitSync(referencedSegments []string, referencedLargeSegments []string) {
	for _, segment := range s.filterCachedSegments(referencedSegments) {
		go s.SynchronizeSegment(segment, nil) // send segment to workerpool (queue is bypassed)
	}

	for _, largeSegment := range s.filterCachedLargeSegments(referencedLargeSegments) {
		go s.SynchronizeLargeSegment(largeSegment, nil)
	}
}

func (s *SynchronizerImpl) synchronizeLargeSegments() error {
	if s.largeSegmentLazyLoad {
		go s.workers.LargeSegmentUpdater.SynchronizeLargeSegments()
		return nil
	}

	return s.workers.LargeSegmentUpdater.SynchronizeLargeSegments()
}
