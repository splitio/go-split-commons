package push

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/service/api/sse"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/common"
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
	// BackoffSSE backoff is running for connecting to stream
	BackoffSSE
	// Error represents some error in SSE streaming
	Error
)

// PushManager strcut for managing push services
type PushManager struct {
	authClient      service.AuthClient
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

	streamingStatus := make(chan int, 1000)
	return &PushManager{
		authClient:      authClient,
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

func (p *PushManager) cancelStreaming() {
	p.logger.Error("Error authenticating, switching to polling")
	p.managerStatus <- Error
}

// Start push services
func (p *PushManager) Start() {
	errResult := make(chan error, 100)
	tokenResult := make(chan *dtos.Token, 100)
	cancelAuthBackoff := common.WithBackoffCancelling(1*time.Second, func() bool {
		token, err := p.authClient.Authenticate()
		if err != nil {
			errType, ok := err.(dtos.HTTPError)
			if ok && errType.Code >= http.StatusInternalServerError {
				p.managerStatus <- BackoffAuth
				return false // It will continue retrying
			}
			errResult <- errors.New("Error authenticating")
			return true
		}
		tokenResult <- token
		return true // Result is OK, Stopping Here, no more backoff
	})

	var token *dtos.Token
	select {
	case tokenAuth := <-tokenResult:
		token = tokenAuth
	case err := <-errResult:
		p.logger.Error(err.Error())
		p.cancelStreaming()
		return
	case <-time.After(1800 * time.Second):
		p.logger.Debug("Authenticator timed out")
		cancelAuthBackoff()
		p.cancelStreaming()
		return
	}

	if !token.PushEnabled {
		p.cancelStreaming()
		return
	}
	channels, err := token.ChannelList()
	if err != nil {
		p.cancelStreaming()
		return
	}

	sseResult := make(chan struct{}, 100)
	cancelSSEConnection := common.WithBackoffCancelling(1*time.Second, func() bool {
		p.sseClient.ConnectStreaming(token.Token, channels, p.eventHandler.HandleIncomingMessage)
		status := <-p.streamingStatus
		switch status {
		case sseStatus.OK:
			sseResult <- struct{}{}
			return true
		case sseStatus.ErrorInternal:
			p.managerStatus <- BackoffSSE
			return false // It will continue retrying
		default:
			errResult <- errors.New("Error connecting streaming")
			return true
		}
	})

	select {
	case <-sseResult:
		break
	case err := <-errResult:
		p.logger.Error(err.Error())
		p.cancelStreaming()
		return
	case <-time.After(1800 * time.Second):
		p.logger.Debug("Streaming timed out")
		cancelSSEConnection()
		p.cancelStreaming()
		return
	}

	p.splitWorker.Start()
	p.segmentWorker.Start()
	p.managerStatus <- Ready

	go func() {
		for {
			select {
			case <-p.streamingStatus:
				p.cancelStreaming()
				return
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
