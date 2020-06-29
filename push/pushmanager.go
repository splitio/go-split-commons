package push

import (
	"errors"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api/sse"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/logging"
	sseStatus "github.com/splitio/go-toolkit/sse"
)

const (
	resetTimer = 120
)

const (
	// Ready represents ready
	Ready = iota
	// PushIsDown there are no publishers for streaming
	PushIsDown
	// PushIsUp there are publishers presents
	PushIsUp
	// Error represents some error in SSE streaming
	Error
)

// PushManager strcut for managing push services
type PushManager struct {
	sseClient       *sse.StreamingClient
	segmentWorker   *SegmentUpdateWorker
	splitWorker     *SplitUpdateWorker
	parser          *NotificationParser
	managerStatus   chan int
	streamingStatus chan int
	publishers      chan int
	logger          logging.LoggerInterface
}

// Missing token exp

// NewPushManager creates new PushManager
func NewPushManager(
	logger logging.LoggerInterface,
	synchronizeSegmentHandler func(segmentName string, till *int64) error,
	synchronizeSplitsHandler func(till *int64) error,
	splitStorage storage.SplitStorage,
	config *conf.AdvancedConfig,
	managerStatus chan int,
) (*PushManager, error) {
	splitQueue := make(chan dtos.SplitChangeNotification, config.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, config.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, splitStorage, logger)
	if err != nil {
		return nil, err
	}
	publishers := make(chan int, 1)
	keeper := NewKeeper(publishers)
	parser := NewNotificationParser(processor, keeper, logger)
	if parser == nil {
		return nil, errors.New("Could not instantiate NotificationParser")
	}
	segmentWorker, err := NewSegmentUpdateWorker(segmentQueue, synchronizeSegmentHandler, logger)
	if err != nil {
		return nil, err
	}
	splitWorker, err := NewSplitUpdateWorker(splitQueue, synchronizeSplitsHandler, logger)
	if err != nil {
		return nil, err
	}

	streamingStatus := make(chan int, 1)
	return &PushManager{
		sseClient:       sse.NewStreamingClient(config, streamingStatus, logger),
		segmentWorker:   segmentWorker,
		splitWorker:     splitWorker,
		managerStatus:   managerStatus,
		streamingStatus: streamingStatus,
		publishers:      publishers,
		parser:          parser,
		logger:          logger,
	}, nil
}

// Start push services
func (p *PushManager) Start(token string, channels []string) {
	go p.sseClient.ConnectStreaming(token, channels, p.parser.HandleIncomingMessage)

	go func() {
		for {
			select {
			case status := <-p.streamingStatus:
				switch status {
				case sseStatus.OK:
					p.splitWorker.Start()
					p.segmentWorker.Start()
					p.managerStatus <- Ready
				default:
					p.managerStatus <- Error
					return
				}
			case publisherStatus := <-p.publishers:
				switch publisherStatus {
				case PublisherNotPresent:
					p.managerStatus <- PushIsDown
				case PublisherAvailable:
					p.managerStatus <- PushIsUp
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
