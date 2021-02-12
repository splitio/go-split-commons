package push

import (
	"fmt"
	"net/http"
	"time"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-split-commons/v2/dtos"
	"github.com/splitio/go-split-commons/v2/service"
	"github.com/splitio/go-split-commons/v2/service/api/sse"
	"github.com/splitio/go-toolkit/v3/common"
	"github.com/splitio/go-toolkit/v3/logging"
	gtSync "github.com/splitio/go-toolkit/v3/sync"
)

// Status update contants that will be propagated to the push manager's user
const (
	StatusUp = iota
	StatusDown
	StatusRetryableError
	StatusNonRetryableError
)

// Manager interface contains public methods for push manager
type Manager interface {
	Start()
	Stop()
	StopWorkers()
	StartWorkers()
}

// ManagerImpl implements the manager interface
type ManagerImpl struct {
	parser        NotificationParser
	sseClient     sse.StreamingClient
	authAPI       service.AuthClient
	processor     Processor
	statusTracker StatusTracker
	feedback      FeedbackLoop
	running       *gtSync.AtomicBool
	nextRefresh   *time.Timer
	logger        logging.LoggerInterface
}

// FeedbackLoop is a type alias for the type of chan that must be supplied for push status tobe propagated
type FeedbackLoop = chan<- int64

// NewManager constructs a new push manager
func NewManager(
	logger logging.LoggerInterface,
	synchronizer synchronizerInterface,
	cfg *conf.AdvancedConfig,
	feedbackLoop chan<- int64,
	authAPI service.AuthClient,
) (*ManagerImpl, error) {

	processor, err := NewProcessor(cfg.SplitUpdateQueueSize, cfg.SegmentUpdateQueueSize, synchronizer, logger)
	if err != nil {
		return nil, fmt.Errorf("error instantiating processor: %w", err)
	}

	statusTracker := NewStatusTracker(logger)
	parser := &NotificationParserImpl{
		logger:            logger,
		onSplitUpdate:     processor.ProcessSplitChangeUpdate,
		onSplitKill:       processor.ProcessSplitKillUpdate,
		onSegmentUpdate:   processor.ProcessSegmentChangeUpdate,
		onControlUpdate:   statusTracker.HandleControl,
		onOccupancyMesage: statusTracker.HandleOccupancy,
		onAblyError:       statusTracker.HandleAblyError,
	}

	return &ManagerImpl{
		authAPI:       authAPI,
		sseClient:     sse.NewStreamingClient(cfg, logger),
		statusTracker: statusTracker,
		running:       gtSync.NewAtomicBool(false),
		feedback:      feedbackLoop,
		processor:     processor,
		parser:        parser,
		logger:        logger,
	}, nil
}

// Start initiates the authentication flow and if successful initiates a connection
func (m *ManagerImpl) Start() {
	m.authAndConenctFlow()
}

// Stop method stops the sse client and it's status monitoring goroutine
func (m *ManagerImpl) Stop() {
	m.statusTracker.NotifySSEShutdownExpected()
	if m.nextRefresh != nil {
		m.nextRefresh.Stop()
	}
	m.sseClient.StopStreaming()
	m.StopWorkers()
}

// StartWorkers start the splits & segments workers
func (m *ManagerImpl) StartWorkers() {
	m.processor.StartWorkers()
}

// StopWorkers stops the splits & segments workers
func (m *ManagerImpl) StopWorkers() {
	m.processor.StopWorkers()
}

func (m *ManagerImpl) performAuthentication() (*dtos.Token, *int64) {
	token, err := m.authAPI.Authenticate()
	if err != nil {
		errType, ok := err.(dtos.HTTPError)
		if ok && errType.Code >= http.StatusInternalServerError {
			m.logger.Error("error authenticating: %s", err)
			return nil, common.Int64Ref(StatusRetryableError)
		}
		return nil, common.Int64Ref(StatusNonRetryableError) // 400, 401, etc
	}
	if !token.PushEnabled {
		return nil, common.Int64Ref(StatusNonRetryableError)
	}
	return token, nil
}

func (m *ManagerImpl) eventHandler(e sse.IncomingMessage) {
	newStatus, err := m.parser.ParseAndForward(e)
	if newStatus != nil {
		m.feedback <- *newStatus
	} else if err != nil {
		m.logger.Error("error parsing message: ", err)
		m.logger.Debug("failed message: ", e)
		m.feedback <- StatusRetryableError
	}
}

func (m *ManagerImpl) authAndConenctFlow() {
	token, status := m.performAuthentication()
	if status != nil {
		m.feedback <- *status
		return
	}

	m.statusTracker.Reset()
	sseStatus := make(chan int, 100)

	tokenList, err := token.ChannelList()
	if err != nil {
		m.logger.Error("error parsing channel list: ", err)
		m.feedback <- StatusRetryableError
		return
	}
	m.sseClient.ConnectStreaming(token.Token, sseStatus, tokenList, m.eventHandler)

	go func() {
		for {
			message := <-sseStatus
			switch message {
			case sse.StatusFirstEventOk:
				m.logger.Info("Streaming Ready")
				m.feedback <- StatusUp

				when, err := token.CalculateNextTokenExpiration()
				if err != nil {
					m.logger.Warning("Failed to calculate next token expiration time. Defaulting to 50 minutes")
					when = 50 * time.Minute
				}
				m.nextRefresh = time.AfterFunc(when, func() {
					m.Stop()
					m.authAndConenctFlow()
				})
			case sse.StatusConnectionFailed:
				m.logger.Error("SSE Connection failed")
				m.feedback <- StatusRetryableError
				return
			case sse.StatusDisconnected:
				m.logger.Debug("propagating sse disconnection event")
				status := m.statusTracker.HandleDisconnection()
				if status != nil {
					m.feedback <- *status
				}
				return
			case sse.StatusUnderlyingClientInUse:
				m.logger.Error("unexpected error in streaming. Switching to polling")
				m.feedback <- StatusNonRetryableError
				return

			}
		}
	}()
}
