package push

import (
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api/sse"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
)

const (
	resetTimer = 120
)

const (
	// Ready represents ready
	Ready = iota
	// PublisherNotPresent there are no publishers sending data
	PublisherNotPresent
	// PublisherAvailable there are publishers running
	PublisherAvailable
	// Error represents some error in SSE streaming
	Error
)

// PushManager strcut for managing push services
type PushManager struct {
	sseClient         *sse.StreamingClient
	processor         *Processor
	segmentWorker     *SegmentUpdateWorker
	splitWorker       *SplitUpdateWorker
	status            chan int
	sseReady          chan struct{}
	sseError          chan error
	keepAlive         chan struct{}
	runningPublishers chan int
	logger            logging.LoggerInterface
}

// Missing token exp

// NewPushManager creates new PushManager
func NewPushManager(
	logger logging.LoggerInterface,
	synchronizeSegmentHandler func(segmentName string, till *int64) error,
	synchronizeSplitsHandler func(till *int64) error,
	splitStorage storage.SplitStorage,
	config *conf.AdvancedConfig,
	status chan int,
) (*PushManager, error) {
	splitQueue := make(chan dtos.SplitChangeNotification, config.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, config.SegmentUpdateQueueSize)
	keepAlive := make(chan struct{}, 1)
	runningPublishers := make(chan int, 1)
	keeper := NewKeeper(runningPublishers)
	processor, err := NewProcessor(segmentQueue, splitQueue, splitStorage, keepAlive, *keeper, logger)
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
		sseClient:         sse.NewStreamingClient(config, sseReady, sseError, logger),
		processor:         processor,
		segmentWorker:     segmentWorker,
		splitWorker:       splitWorker,
		sseReady:          sseReady,
		sseError:          sseError,
		status:            status,
		keepAlive:         keepAlive,
		runningPublishers: runningPublishers,
		logger:            logger,
	}, nil
}

// Start push services
func (p *PushManager) Start(token string, channels []string) {
	go p.sseClient.ConnectStreaming(token, channels, p.processor.HandleIncomingMessage)

	go func() {
		for {
			select {
			case <-p.sseReady:
				p.splitWorker.Start()
				p.segmentWorker.Start()
				p.status <- Ready
				keepRunning := true
				for keepRunning {
					select {
					case <-time.After(resetTimer * time.Second):
						keepRunning = false
						p.status <- Error
					case <-p.keepAlive:
					}
				}
			case <-p.sseError:
				p.logger.Error("Some error occured when connecting to streaming")
				p.status <- Error
				return
			case publishers := <-p.runningPublishers:
				p.logger.Debug("Publishers updated", publishers)
				if publishers == 0 {
					p.status <- PublisherNotPresent
				} else {
					p.status <- PublisherAvailable
				}
			}
		}
	}()
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
