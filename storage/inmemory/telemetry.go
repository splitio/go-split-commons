package inmemory

import (
	"sync"
	"sync/atomic"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage"
)

type latencies struct {
	// MethodLatencies
	treatment            AtomicInt64Slice
	treatments           AtomicInt64Slice
	treatmentWithConfig  AtomicInt64Slice
	treatmentWithConfigs AtomicInt64Slice
	track                AtomicInt64Slice

	// HTTPLatencies
	splits      AtomicInt64Slice
	segments    AtomicInt64Slice
	impressions AtomicInt64Slice
	events      AtomicInt64Slice
	telemetry   AtomicInt64Slice
	token       AtomicInt64Slice
}

type counters struct {
	// Evaluation Counters
	treatment            int64
	treatments           int64
	treatmentWithConfig  int64
	treatmentWithConfigs int64
	track                int64

	// Push Counters
	authRejections int64
	tokenRefreshes int64

	// Factory Counters
	burTimeouts    int64
	nonReadyUsages int64
}

type records struct {
	// Impressions Data
	impressionsQueued  int64
	impressionsDropped int64
	impressionsDeduped int64

	// Events Data
	eventsQueued  int64
	eventsDropped int64

	// LastSynchronization
	splits      int64
	segments    int64
	impressions int64
	events      int64
	token       int64
	telemetry   int64

	// SDK
	session int64

	// Factory
	timeUntilReady int64
}

// IMTelemetryStorage In Memoty Telemetry Storage struct
type IMTelemetryStorage struct {
	counters             counters
	httpErrors           dtos.HTTPErrors
	latencies            latencies
	records              records
	streamingEvents      []dtos.StreamingEvent // Max Length 20
	mutexStreamingEvents sync.RWMutex
	tags                 []string
	mutexTags            sync.RWMutex
}

// NewIMTelemetryStorage builds in memory telemetry storage
func NewIMTelemetryStorage() storage.TelemetryStorage {
	treatmentLatencies, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	treatmentWithConfigLatencies, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	treatmentsLatencies, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	treatmentWithConfigsLatencies, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	track, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}

	splits, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	segments, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	impressions, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	events, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	telemetry, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}
	token, err := NewAtomicInt64Slice(storage.LatencyBucketCount)
	if err != nil {
		return nil
	}

	return &IMTelemetryStorage{
		counters: counters{},
		httpErrors: dtos.HTTPErrors{
			Splits:      make(map[int]int64),
			Segments:    make(map[int]int64),
			Impressions: make(map[int]int64),
			Events:      make(map[int]int64),
			Token:       make(map[int]int64),
			Telemetry:   make(map[int]int64),
		},
		latencies: latencies{
			treatment:            treatmentLatencies,
			treatmentWithConfig:  treatmentWithConfigLatencies,
			treatments:           treatmentsLatencies,
			treatmentWithConfigs: treatmentWithConfigsLatencies,
			track:                track,

			splits:      splits,
			segments:    segments,
			impressions: impressions,
			events:      events,
			token:       token,
			telemetry:   telemetry,
		},
		records:              records{},
		streamingEvents:      make([]dtos.StreamingEvent, 0, storage.MaxStreamingEvents),
		mutexStreamingEvents: sync.RWMutex{},
		tags:                 make([]string, 0, storage.MaxTags),
		mutexTags:            sync.RWMutex{},
	}
}

// TELEMETRY STORAGE PRODUCER

// RecordLatency stores latency for method
func (i *IMTelemetryStorage) RecordLatency(method int, bucket int) {
	switch method {
	case storage.Treatment:
		i.latencies.treatment.Incr(bucket)
	case storage.Treatments:
		i.latencies.treatments.Incr(bucket)
	case storage.TreatmentWithConfig:
		i.latencies.treatmentWithConfig.Incr(bucket)
	case storage.TreatmentsWithConfig:
		i.latencies.treatmentWithConfigs.Incr(bucket)
	case storage.Track:
		i.latencies.track.Incr(bucket)
	}
}

// RecordException stores exceptions for method
func (i *IMTelemetryStorage) RecordException(method int) {
	switch method {
	case storage.Treatment:
		atomic.AddInt64(&i.counters.treatment, 1)
	case storage.Treatments:
		atomic.AddInt64(&i.counters.treatments, 1)
	case storage.TreatmentWithConfig:
		atomic.AddInt64(&i.counters.treatmentWithConfig, 1)
	case storage.TreatmentsWithConfig:
		atomic.AddInt64(&i.counters.treatmentWithConfigs, 1)
	case storage.Track:
		atomic.AddInt64(&i.counters.track, 1)
	}
}

// RecordImpressionsStats records impressions by type
func (i *IMTelemetryStorage) RecordImpressionsStats(dataType int, count int64) {
	switch dataType {
	case storage.ImpressionsDropped:
		atomic.AddInt64(&i.records.impressionsDropped, count)
	case storage.ImpressionsDeduped:
		atomic.AddInt64(&i.records.impressionsDeduped, count)
	case storage.ImpressionsQueued:
		atomic.AddInt64(&i.records.impressionsQueued, count)
	}
}

// RecordEventsStats recirds events by type
func (i *IMTelemetryStorage) RecordEventsStats(dataType int, count int64) {
	switch dataType {
	case storage.EventsDropped:
		atomic.AddInt64(&i.records.eventsDropped, count)
	case storage.EventsQueued:
		atomic.AddInt64(&i.records.eventsQueued, count)
	}
}

// RecordSuccessfulSync records sync for resource
func (i *IMTelemetryStorage) RecordSuccessfulSync(resource int, timestamp int64) {
	switch resource {
	case storage.SplitSync:
		atomic.StoreInt64(&i.records.splits, timestamp)
	case storage.SegmentSync:
		atomic.StoreInt64(&i.records.segments, timestamp)
	case storage.ImpressionSync:
		atomic.StoreInt64(&i.records.impressions, timestamp)
	case storage.EventSync:
		atomic.StoreInt64(&i.records.events, timestamp)
	case storage.TelemetrySync:
		atomic.StoreInt64(&i.records.telemetry, timestamp)
	case storage.TokenSync:
		atomic.StoreInt64(&i.records.token, timestamp)
	}
}

func (i *IMTelemetryStorage) createOrUpdate(status int, item map[int]int64) {
	if item == nil {
		item[status] = 1
		return
	}
	item[status]++
}

// RecordSyncError records http error
func (i *IMTelemetryStorage) RecordSyncError(resource int, status int) {
	switch resource {
	case storage.SplitSync:
		i.createOrUpdate(status, i.httpErrors.Splits)
	case storage.SegmentSync:
		i.createOrUpdate(status, i.httpErrors.Segments)
	case storage.ImpressionSync:
		i.createOrUpdate(status, i.httpErrors.Impressions)
	case storage.EventSync:
		i.createOrUpdate(status, i.httpErrors.Events)
	case storage.TelemetrySync:
		i.createOrUpdate(status, i.httpErrors.Telemetry)
	case storage.TokenSync:
		i.createOrUpdate(status, i.httpErrors.Token)
	}
}

// RecordSyncLatency records http error
func (i *IMTelemetryStorage) RecordSyncLatency(resource int, bucket int) {
	switch resource {
	case storage.SplitSync:
		i.latencies.splits.Incr(bucket)
	case storage.SegmentSync:
		i.latencies.segments.Incr(bucket)
	case storage.ImpressionSync:
		i.latencies.impressions.Incr(bucket)
	case storage.EventSync:
		i.latencies.events.Incr(bucket)
	case storage.TelemetrySync:
		i.latencies.telemetry.Incr(bucket)
	case storage.TokenSync:
		i.latencies.token.Incr(bucket)
	}
}

// RecordAuthRejections records auth rejections
func (i *IMTelemetryStorage) RecordAuthRejections() {
	atomic.AddInt64(&i.counters.authRejections, 1)
}

// RecordTokenRefreshes records token
func (i *IMTelemetryStorage) RecordTokenRefreshes() {
	atomic.AddInt64(&i.counters.tokenRefreshes, 1)
}

// RecordStreamingEvent appends new streaming event
func (i *IMTelemetryStorage) RecordStreamingEvent(event dtos.StreamingEvent) {
	i.mutexStreamingEvents.Lock()
	defer i.mutexStreamingEvents.Unlock()
	if len(i.streamingEvents) < storage.MaxStreamingEvents {
		i.streamingEvents = append(i.streamingEvents, event)
	}
}

// AddTag adds particular tag
func (i *IMTelemetryStorage) AddTag(tag string) {
	i.mutexTags.Lock()
	defer i.mutexTags.Unlock()
	if len(i.tags) < storage.MaxTags {
		i.tags = append(i.tags, tag)
	}
}

// RecordSessionLength records session length
func (i *IMTelemetryStorage) RecordSessionLength(session int64) {
	atomic.StoreInt64(&i.records.session, session)
}

// RecordNonReadyUsage records non ready usage
func (i *IMTelemetryStorage) RecordNonReadyUsage() {
	atomic.AddInt64(&i.counters.nonReadyUsages, 1)
}

// RecordBURTimeout records bur timeodout
func (i *IMTelemetryStorage) RecordBURTimeout() {
	atomic.AddInt64(&i.counters.burTimeouts, 1)
}

// TELEMETRY STORAGE CONSUMER

// PopLatencies gets and clears method latencies
func (i *IMTelemetryStorage) PopLatencies() dtos.MethodLatencies {
	return dtos.MethodLatencies{
		Treatment:            i.latencies.treatment.FetchAndClearAll(),
		Treatments:           i.latencies.treatments.FetchAndClearAll(),
		TreatmentWithConfig:  i.latencies.treatmentWithConfig.FetchAndClearAll(),
		TreatmentWithConfigs: i.latencies.treatmentWithConfigs.FetchAndClearAll(),
		Track:                i.latencies.track.FetchAndClearAll(),
	}
}

// PopExceptions gets and clears method exceptions
func (i *IMTelemetryStorage) PopExceptions() dtos.MethodExceptions {
	return dtos.MethodExceptions{
		Treatment:            atomic.SwapInt64(&i.counters.treatment, 0),
		Treatments:           atomic.SwapInt64(&i.counters.treatments, 0),
		TreatmentWithConfig:  atomic.SwapInt64(&i.counters.treatmentWithConfig, 0),
		TreatmentWithConfigs: atomic.SwapInt64(&i.counters.treatmentWithConfigs, 0),
		Track:                atomic.SwapInt64(&i.counters.track, 0),
	}
}

// GetImpressionsStats gets impressions by type
func (i *IMTelemetryStorage) GetImpressionsStats(dataType int) int64 {
	switch dataType {
	case storage.ImpressionsDropped:
		return atomic.LoadInt64(&i.records.impressionsDropped)
	case storage.ImpressionsDeduped:
		return atomic.LoadInt64(&i.records.impressionsDeduped)
	case storage.ImpressionsQueued:
		return atomic.LoadInt64(&i.records.impressionsQueued)
	}
	return 0
}

// GetEventsStats gets events by type
func (i *IMTelemetryStorage) GetEventsStats(dataType int) int64 {
	switch dataType {
	case storage.EventsDropped:
		return atomic.LoadInt64(&i.records.eventsDropped)
	case storage.EventsQueued:
		return atomic.LoadInt64(&i.records.eventsQueued)
	}
	return 0
}

// GetLastSynchronization gets last synchronization stats for fetchers and recorders
func (i *IMTelemetryStorage) GetLastSynchronization() dtos.LastSynchronization {
	return dtos.LastSynchronization{
		Splits:      atomic.LoadInt64(&i.records.splits),
		Segments:    atomic.LoadInt64(&i.records.segments),
		Impressions: atomic.LoadInt64(&i.records.impressions),
		Events:      atomic.LoadInt64(&i.records.events),
		Telemetry:   atomic.LoadInt64(&i.records.telemetry),
		Token:       atomic.LoadInt64(&i.records.token),
	}
}

// PopHTTPErrors gets http errors
func (i *IMTelemetryStorage) PopHTTPErrors() dtos.HTTPErrors {
	toReturn := i.httpErrors
	i.httpErrors.Splits = make(map[int]int64)
	i.httpErrors.Segments = make(map[int]int64)
	i.httpErrors.Impressions = make(map[int]int64)
	i.httpErrors.Events = make(map[int]int64)
	i.httpErrors.Telemetry = make(map[int]int64)
	i.httpErrors.Token = make(map[int]int64)
	return toReturn
}

// PopHTTPLatencies gets http latencies
func (i *IMTelemetryStorage) PopHTTPLatencies() dtos.HTTPLatencies {
	return dtos.HTTPLatencies{
		Splits:      i.latencies.splits.FetchAndClearAll(),
		Segments:    i.latencies.segments.FetchAndClearAll(),
		Impressions: i.latencies.impressions.FetchAndClearAll(),
		Events:      i.latencies.events.FetchAndClearAll(),
		Telemetry:   i.latencies.telemetry.FetchAndClearAll(),
		Token:       i.latencies.token.FetchAndClearAll(),
	}
}

// PopAuthRejections gets total amount of auth rejections
func (i *IMTelemetryStorage) PopAuthRejections() int64 {
	return atomic.SwapInt64(&i.counters.authRejections, 0)
}

// PopTokenRefreshes gets total amount of token refreshes
func (i *IMTelemetryStorage) PopTokenRefreshes() int64 {
	return atomic.SwapInt64(&i.counters.tokenRefreshes, 0)
}

// PopStreamingEvents gets streamingEvents data
func (i *IMTelemetryStorage) PopStreamingEvents() []dtos.StreamingEvent {
	i.mutexStreamingEvents.Lock()
	defer i.mutexStreamingEvents.Unlock()
	toReturn := i.streamingEvents
	i.streamingEvents = make([]dtos.StreamingEvent, 0, storage.MaxStreamingEvents)
	return toReturn
}

// PopTags gets total amount of tags
func (i *IMTelemetryStorage) PopTags() []string {
	i.mutexTags.Lock()
	defer i.mutexTags.Unlock()
	toReturn := i.tags
	i.tags = make([]string, 0, storage.MaxTags)
	return toReturn
}

// GetSessionLength gets session duration
func (i *IMTelemetryStorage) GetSessionLength() int64 {
	return atomic.LoadInt64(&i.records.session)
}

// GetNonReadyUsages gets non usages on ready
func (i *IMTelemetryStorage) GetNonReadyUsages() int64 {
	return atomic.LoadInt64(&i.counters.nonReadyUsages)
}

// GetBURTimeouts gets timedouts data
func (i *IMTelemetryStorage) GetBURTimeouts() int64 {
	return atomic.LoadInt64(&i.counters.burTimeouts)
}
