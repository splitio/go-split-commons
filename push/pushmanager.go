package push

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/processor"
	"github.com/splitio/go-split-commons/service/api/sse"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

// PushManager strcut for managing push services
type PushManager struct {
	sseClient     *sse.StreamingClient
	processor     *processor.Processor
	segmentWorker *SegmentUpdateWorker
	splitWorker   *SplitUpdateWorker
	logger        logging.LoggerInterface
}

// Missing token exp

// NewPushManager creates new PushManager
func NewPushManager(
	logger logging.LoggerInterface,
	synchronizeSegmentHandler func(segmentName string, till *int64) error,
	synchronizeSplitsHandler func(till *int64) error,
	splitStorage storage.SplitStorage,
	config *conf.AdvancedConfig,
) *PushManager {
	splitQueue := make(chan dtos.SplitChangeNotification, 5000)
	segmentQueue := make(chan dtos.SegmentChangeNotification, 5000)
	processor, err := processor.NewProcessor(segmentQueue, splitQueue, splitStorage, logger)
	if err != nil {
		logger.Error("Err creating processor", err)
	}
	segmentWorker, err := NewSegmentUpdateWorker(segmentQueue, synchronizeSegmentHandler, logger)
	if err != nil {
		logger.Error("Err creating segmentWorker", err)
	}
	splitWorker, err := NewSplitUpdateWorker(splitQueue, synchronizeSplitsHandler, logger)
	if err != nil {
		logger.Error("Err creating splitWorker", err)
	}

	return &PushManager{
		sseClient:     sse.NewStreamingClient(config, make(chan struct{}, 1), logger),
		processor:     processor,
		segmentWorker: segmentWorker,
		splitWorker:   splitWorker,
		logger:        logger,
	}
}

// Start push services
func (p *PushManager) Start(token string, channels []string) error {
	p.logger.Info("CHANNELS:", channels)
	go p.sseClient.ConnectStreaming(token, channels, p.processor.HandleIncomingMessage)

	p.splitWorker.Start()
	p.segmentWorker.Start()
	return nil
}

// Stop push services
func (p *PushManager) Stop() {
	p.logger.Error("CALLED STOP")
	p.sseClient.StopStreaming()
	p.splitWorker.Stop()
	p.segmentWorker.Stop()
}
