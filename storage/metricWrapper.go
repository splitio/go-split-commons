package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/splitio/go-toolkit/logging"
)

// MetricWrapper struct
type MetricWrapper struct {
	Telemetry     MetricsStorage
	LocalTelemtry MetricsStorage
	Logger        logging.LoggerInterface
	dictionary    map[string]string
}

const (
	// SplitChangesCounter counters
	SplitChangesCounter = iota
	// SplitChangesLatency latencies
	SplitChangesLatency
	// SegmentChangesCounter counters
	SegmentChangesCounter
	// SegmentChangesLatency latencies
	SegmentChangesLatency
	// TestImpressionsCounter counter
	TestImpressionsCounter
	// TestImpressionsLatency latencies
	TestImpressionsLatency
	// PostEventsCounter counter
	PostEventsCounter
	//PostEventsLatency latencies
	PostEventsLatency
	// MySegmentsCounter counters
	MySegmentsCounter
	// MySegmentsLatency latencies
	MySegmentsLatency
)

const (
	proxyError = "request.error"
	proxyOk    = "request.ok"

	localCounter = "request.{status}"

	splitChangesCounter      = "splitChangeFetcher.status.{status}"
	splitChangesLatency      = "splitChangeFetcher.time"
	localSplitChangesLatency = "/api/splitChanges"

	segmentChangesCounter      = "segmentChangeFetcher.status.{status}"
	segmentChangesLatency      = "segmentChangeFetcher.time"
	localSegmentChangesLatency = "/api/segmentChanges"

	testImpressionsCounter      = "testImpressions.status.{status}"
	testImpressionsLatency      = "testImpressions.time"
	localTestImpressionsLatency = "/api/testImpressions/bulk"

	postEventsCounter      = "events.status.{status}"
	postEventsLatency      = "events.time"
	localPostEventsLatency = "/api/events/bulk"

	mySegmentsCounter      = "mySegments.status.{status}"
	mySegmentsLatency      = "mySegments.time"
	localMySegmentsLatency = "/api/mySegments"
)

// NewMetricWrapper builds new wrapper
func NewMetricWrapper(telemetry MetricsStorage, localTelemtry MetricsStorage, logger logging.LoggerInterface) *MetricWrapper {
	return &MetricWrapper{
		LocalTelemtry: localTelemtry,
		Logger:        logger,
		Telemetry:     telemetry,
	}
}

func (m *MetricWrapper) getKey(key int) (string, string, error) {
	switch key {
	case SplitChangesCounter:
		return splitChangesCounter, localCounter, nil
	case SplitChangesLatency:
		return splitChangesLatency, localSplitChangesLatency, nil
	case SegmentChangesCounter:
		return segmentChangesCounter, localCounter, nil
	case SegmentChangesLatency:
		return segmentChangesLatency, localSegmentChangesLatency, nil
	case TestImpressionsCounter:
		return testImpressionsCounter, localCounter, nil
	case TestImpressionsLatency:
		return testImpressionsLatency, localTestImpressionsLatency, nil
	case PostEventsCounter:
		return postEventsCounter, localCounter, nil
	case PostEventsLatency:
		return postEventsLatency, localPostEventsLatency, nil
	case MySegmentsCounter:
		return mySegmentsCounter, localCounter, nil
	case MySegmentsLatency:
		return mySegmentsLatency, localMySegmentsLatency, nil
	default:
		return "", "", errors.New("Key does not exist")
	}
}

// StoreCounters stores counters
func (m *MetricWrapper) StoreCounters(key int, value string, proxyMode bool) {
	common, local, err := m.getKey(key)
	if err != nil {
		return
	}
	if m.LocalTelemtry != nil {
		if proxyMode {
			status, _ := strconv.ParseInt(value, 10, 64)
			key := proxyOk
			if status != 200 {
				key = proxyError
			}
			m.LocalTelemtry.IncCounter(key)
		} else {
			m.LocalTelemtry.IncCounter(fmt.Sprintf("backend::%s", strings.Replace(local, "{status}", value, 1)))
		}
	}
	if value == "ok" {
		value = "200"
	}
	m.Telemetry.IncCounter(strings.Replace(common, "{status}", value, 1))
}

// StoreLatencies stores counters
func (m *MetricWrapper) StoreLatencies(key int, bucket int, proxyMode bool) {
	common, local, err := m.getKey(key)
	if err != nil {
		return
	}
	if m.LocalTelemtry != nil {
		if proxyMode {
			m.LocalTelemtry.IncLatency(local, bucket)
		} else {
			m.LocalTelemtry.IncLatency(fmt.Sprintf("backend::%s", local), bucket)
		}
	}
	m.Telemetry.IncLatency(common, bucket)
}
