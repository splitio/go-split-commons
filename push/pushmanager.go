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
	sseReady      chan struct{}
	sseError      chan error
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
) (*PushManager, error) {
	splitQueue := make(chan dtos.SplitChangeNotification, config.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, config.SegmentUpdateQueueSize)
	processor, err := processor.NewProcessor(segmentQueue, splitQueue, splitStorage, logger)
	if err != nil {
		return nil, err
	}
	segmentWorker, err := NewSegmentUpdateWorker(segmentQueue, synchronizeSegmentHandler, logger)
	if err != nil {
		return nil, err
	}
	splitWorker, err := NewSplitUpdateWorker(splitQueue, synchronizeSplitsHandler, logger)
	if err != nil {
		return nil, err
	}

	sseReady := make(chan struct{}, 1)
	sseError := make(chan error, 1)
	return &PushManager{
		sseClient:     sse.NewStreamingClient(config, sseReady, sseError, logger),
		processor:     processor,
		segmentWorker: segmentWorker,
		splitWorker:   splitWorker,
		sseReady:      sseReady,
		sseError:      sseError,
		logger:        logger,
	}, nil
}

// Start push services
func (p *PushManager) Start(token string, channels []string) error {
	go p.sseClient.ConnectStreaming(token, channels, p.processor.HandleIncomingMessage)

	for {
		select {
		case <-p.sseReady:
			p.splitWorker.Start()
			p.segmentWorker.Start()
			return nil
		case err := <-p.sseError:
			p.logger.Error("Some error occured when connecting to streaming")
			p.Stop()
			return err
		}
	}
}

// Stop push services
func (p *PushManager) Stop() {
	p.logger.Info("Stopping Push Services")
	if p.sseClient.IsRunning() {
		p.sseClient.StopStreaming()
	}
	if p.splitWorker.IsRunning() {
		p.splitWorker.Stop()
	}
	if p.segmentWorker.IsRunning() {
		p.segmentWorker.Stop()
	}
}

// IsRunning returns true if the services are running
func (p *PushManager) IsRunning() bool {
	return p.sseClient.IsRunning() || p.splitWorker.IsRunning() || p.segmentWorker.IsRunning()
}
