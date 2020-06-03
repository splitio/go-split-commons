package api

import (
	"encoding/json"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

type httpRecorderBase struct {
	client *HTTPClient
	logger logging.LoggerInterface
}

func (h *httpRecorderBase) recordRaw(url string, data []byte, metadata dtos.Metadata) error {
	headers := make(map[string]string)
	headers["SplitSDKVersion"] = metadata.SDKVersion
	if metadata.MachineName != "NA" && metadata.MachineName != "unknown" {
		headers["SplitSDKMachineName"] = metadata.MachineName
	}
	if metadata.MachineIP != "NA" && metadata.MachineIP != "unknown" {
		headers["SplitSDKMachineIP"] = metadata.MachineIP
	}
	return h.client.Post(url, data, headers)
}

// HTTPImpressionRecorder is a struct responsible for submitting impression bulks to the backend
type HTTPImpressionRecorder struct {
	httpRecorderBase
}

// Record sends an array (or slice) of impressionsRecord to the backend
func (i *HTTPImpressionRecorder) Record(impressions []dtos.Impression, metadata dtos.Metadata) error {
	impressionsToPost := make(map[string][]dtos.ImpressionDTO)
	for _, impression := range impressions {
		keyImpression := dtos.ImpressionDTO{
			KeyName:      impression.KeyName,
			Treatment:    impression.Treatment,
			Time:         impression.Time,
			ChangeNumber: impression.ChangeNumber,
			Label:        impression.Label,
			BucketingKey: impression.BucketingKey,
		}
		v, ok := impressionsToPost[impression.FeatureName]
		if ok {
			v = append(v, keyImpression)
		} else {
			v = []dtos.ImpressionDTO{keyImpression}
		}
		impressionsToPost[impression.FeatureName] = v
	}

	bulkImpressions := make([]dtos.ImpressionsDTO, 0)
	for testName, testImpressions := range impressionsToPost {
		bulkImpressions = append(bulkImpressions, dtos.ImpressionsDTO{
			TestName:       testName,
			KeyImpressions: testImpressions,
		})
	}

	data, err := json.Marshal(bulkImpressions)
	if err != nil {
		i.logger.Error("Error marshaling JSON", err.Error())
		return err
	}

	err = i.recordRaw("/testImpressions/bulk", data, metadata)
	if err != nil {
		i.logger.Error("Error posting impressions", err.Error())
		return err
	}

	return nil
}

// NewHTTPImpressionRecorder instantiates an HTTPImpressionRecorder
func NewHTTPImpressionRecorder(
	apikey string,
	cfg *conf.AdvancedConfig,
	logger logging.LoggerInterface,
) *HTTPImpressionRecorder {
	_, eventsURL := getUrls(cfg)
	client := NewHTTPClient(apikey, cfg, eventsURL, logger)
	return &HTTPImpressionRecorder{
		httpRecorderBase: httpRecorderBase{
			client: client,
			logger: logger,
		},
	}
}

// HTTPMetricsRecorder is a struct responsible for submitting metrics (latency, gauge, counters) to the backend
type HTTPMetricsRecorder struct {
	httpRecorderBase
}

// RecordCounters method submits counter metrics to the backend
func (m *HTTPMetricsRecorder) RecordCounters(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
	data, err := json.Marshal(counters)
	if err != nil {
		m.logger.Error("Error marshaling JSON", err.Error())
		return err
	}

	err = m.recordRaw("/metrics/counters", data, metadata)
	if err != nil {
		m.logger.Error("Error posting counters", err.Error())
		return err
	}

	return nil
}

// RecordLatencies method submits latency metrics to the backend
func (m *HTTPMetricsRecorder) RecordLatencies(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
	data, err := json.Marshal(latencies)
	if err != nil {
		m.logger.Error("Error marshaling JSON", err.Error())
		return err
	}

	err = m.recordRaw("/metrics/times", data, metadata)
	if err != nil {
		m.logger.Error("Error posting latencies", err.Error())
		return err
	}

	return nil
}

// RecordGauge method submits gauge metrics to the backend
func (m *HTTPMetricsRecorder) RecordGauge(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
	data, err := json.Marshal(gauge)
	if err != nil {
		m.logger.Error("Error marshaling JSON", err.Error())
		return err
	}

	err = m.recordRaw("/metrics/gauge", data, metadata)
	if err != nil {
		m.logger.Error("Error posting gauges", err.Error())
		return err
	}

	return nil
}

// NewHTTPMetricsRecorder instantiates an HTTPMetricsRecorder
func NewHTTPMetricsRecorder(
	apikey string,
	cfg *conf.AdvancedConfig,
	logger logging.LoggerInterface,
) *HTTPMetricsRecorder {
	_, eventsURL := getUrls(cfg)
	client := NewHTTPClient(apikey, cfg, eventsURL, logger)
	return &HTTPMetricsRecorder{
		httpRecorderBase: httpRecorderBase{
			client: client,
			logger: logger,
		},
	}
}

// HTTPEventsRecorder is a struct responsible for submitting events bulks to the backend
type HTTPEventsRecorder struct {
	httpRecorderBase
}

// Record sends an array (or slice) of dtos.EventDTO to the backend
func (i *HTTPEventsRecorder) Record(events []dtos.EventDTO, metadata dtos.Metadata) error {
	data, err := json.Marshal(events)
	if err != nil {
		i.logger.Error("Error marshaling JSON", err.Error())
		return err
	}

	err = i.recordRaw("/events/bulk", data, metadata)
	if err != nil {
		i.logger.Error("Error posting events", err.Error())
		return err
	}

	return nil
}

// NewHTTPEventsRecorder instantiates an HTTPEventsRecorder
func NewHTTPEventsRecorder(
	apikey string,
	cfg *conf.AdvancedConfig,
	logger logging.LoggerInterface,
) *HTTPEventsRecorder {
	_, eventsURL := getUrls(cfg)
	client := NewHTTPClient(apikey, cfg, eventsURL, logger)
	return &HTTPEventsRecorder{
		httpRecorderBase: httpRecorderBase{
			client: client,
			logger: logger,
		},
	}
}
