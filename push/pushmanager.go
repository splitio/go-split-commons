package push

import (
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
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
	// BackoffAuth backoff is running for authentication
	BackoffAuth
	// Error represents some error in SSE streaming
	Error
)

// PushManager strcut for managing push services
type PushManager struct {
	authentication  chan interface{}
	authenticator   *Authenticator
	sseClient       *sse.StreamingClient
	segmentWorker   *SegmentUpdateWorker
	splitWorker     *SplitUpdateWorker
	eventHandler    *EventHandler
	managerStatus   chan<- int
	streamingStatus chan int
	publishers      chan int
	logger          logging.LoggerInterface
}

// NewPushManager creates new PushManager
func NewPushManager(
	logger logging.LoggerInterface,
	synchronizeSegmentHandler func(segmentName string, till *int64) error,
	synchronizeSplitsHandler func(till *int64) error,
	splitStorage storage.SplitStorage,
	config *conf.AdvancedConfig,
	managerStatus chan int,
	authClient service.AuthClient,
) (*PushManager, error) {
	splitQueue := make(chan dtos.SplitChangeNotification, config.SplitUpdateQueueSize)
	segmentQueue := make(chan dtos.SegmentChangeNotification, config.SegmentUpdateQueueSize)
	processor, err := NewProcessor(segmentQueue, splitQueue, splitStorage, logger)
	if err != nil {
		return nil, err
	}
	parser := NewNotificationParser(logger)
	if parser == nil {
		return nil, errors.New("Could not instantiate NotificationParser")
	}
	publishers := make(chan int, 1000)
	keeper := NewKeeper(publishers)
	if keeper == nil {
		return nil, errors.New("Could not instantiate Keeper")
	}
	eventHandler := NewEventHandler(keeper, parser, processor, logger)
	segmentWorker, err := NewSegmentUpdateWorker(segmentQueue, synchronizeSegmentHandler, logger)
	if err != nil {
		return nil, err
	}
	splitWorker, err := NewSplitUpdateWorker(splitQueue, synchronizeSplitsHandler, logger)
	if err != nil {
		return nil, err
	}

	authenticationStatus := make(chan interface{}, 1000)
	authenticator := NewAuthenticator(authenticationStatus, authClient, logger)
	if authenticator == nil {
		return nil, errors.New("Could not start Authenticator")
	}

	streamingStatus := make(chan int, 1000)
	return &PushManager{
		authentication:  authenticationStatus,
		authenticator:   authenticator,
		sseClient:       sse.NewStreamingClient(config, streamingStatus, logger),
		segmentWorker:   segmentWorker,
		splitWorker:     splitWorker,
		managerStatus:   managerStatus,
		streamingStatus: streamingStatus,
		eventHandler:    eventHandler,
		publishers:      publishers,
		logger:          logger,
	}, nil
}

// Missing token exp

func (p *PushManager) switchToPolling() {
	p.logger.Error("Error authenticating, switching to polling")
	p.managerStatus <- Error
	return
}

// Start push services
func (p *PushManager) Start() {
	p.authenticator.Start()

	go func() {
		for {
			select {
			case authenticationStatus := <-p.authentication:
				p.logger.Debug(fmt.Sprintf("Authenticator received event: %v", authenticationStatus))
				p.logger.Debug(fmt.Sprintf("Authenticator received event: %T", authenticationStatus))
				switch v := authenticationStatus.(type) {
				case int:
					switch v {
					case Retrying:
						p.managerStatus <- BackoffAuth
					case Finished:
						p.switchToPolling()
					default:
						p.switchToPolling()
					}
				case *dtos.Token:
					p.logger.Info("Authentication backoff is done, token received")
					if v.PushEnabled {
						channels, err := v.ChannelList()
						if err != nil {
							p.switchToPolling()
						}
						go p.sseClient.ConnectStreaming(v.Token, channels, p.eventHandler.HandleIncomingMessage)
					} else {
						p.switchToPolling()
					}
				default:
					p.switchToPolling()
				}
			case status := <-p.streamingStatus:
				switch status {
				case sseStatus.OK:
					p.splitWorker.Start()
					p.segmentWorker.Start()
					p.managerStatus <- Ready
				default:
					p.switchToPolling()
				}
			case publisherStatus := <-p.publishers:
				switch publisherStatus {
				case PublisherNotPresent:
					p.managerStatus <- PushIsDown
				case PublisherAvailable:
					p.managerStatus <- PushIsUp
				default:
					p.logger.Debug(fmt.Sprintf("Unexpected publisher status received %d", publisherStatus))
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

// StopWorkers stops workers
func (p *PushManager) StopWorkers() {
	if p.splitWorker.IsRunning() {
		p.splitWorker.Stop()
	}
	if p.segmentWorker.IsRunning() {
		p.segmentWorker.Stop()
	}
}

// StartWorkers starts workers
func (p *PushManager) StartWorkers() {
	if !p.splitWorker.IsRunning() {
		p.splitWorker.Start()
	}
	if !p.segmentWorker.IsRunning() {
		p.segmentWorker.Start()
	}
}
