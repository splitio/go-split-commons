package conf

const (
	defaultHTTPTimeout            = 30
	defaultSegmentQueueSize       = 500
	defaultSegmentWorkers         = 10
	defaultEventsBulkSize         = 5000
	defaultEventsQueueSize        = 10000
	defaultImpressionsQueueSize   = 10000
	defaultImpressionsBulkSize    = 5000
	defaultStreamingEnabled       = true
	defaultSplitUpdateQueueSize   = 5000
	defaultSegmentUpdateQueueSize = 5000
)

// GetDefaultAdvancedConfig returns default conf
func GetDefaultAdvancedConfig() AdvancedConfig {
	return AdvancedConfig{
		EventsQueueSize:        defaultEventsQueueSize,
		HTTPTimeout:            defaultHTTPTimeout,
		EventsBulkSize:         defaultEventsBulkSize,
		ImpressionsBulkSize:    defaultImpressionsBulkSize,
		ImpressionsQueueSize:   defaultImpressionsQueueSize,
		SegmentQueueSize:       defaultSegmentQueueSize,
		SegmentUpdateQueueSize: defaultSegmentUpdateQueueSize,
		SegmentWorkers:         defaultSegmentWorkers,
		SplitUpdateQueueSize:   defaultSplitUpdateQueueSize,
		StreamingEnabled:       defaultStreamingEnabled,
		AuthServiceURL:         "",
		EventsURL:              "",
		SdkURL:                 "",
		StreamingServiceURL:    "",
	}
}
