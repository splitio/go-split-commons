package push

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
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

// ErrAlreadyRunning is the error to be returned when .Start() is called on an already running instance
var ErrAlreadyRunning = errors.New("push manager already running")

// ErrNotRunning is the error to be returned when .Stop() is called on a non-running instance
var ErrNotRunning = errors.New("push manager is either shutting down or idle")

// Manager interface contains public methods for push manager
type Manager interface {
	Start() error
	Stop() error
	StopWorkers()
	StartWorkers()
}

// ManagerImpl implements the manager interface
type ManagerImpl struct {
	parser            NotificationParser
	sseClient         sse.StreamingClient
	authAPI           service.AuthClient
	processor         Processor
	statusTracker     StatusTracker
	feedback          FeedbackLoop
	running           *gtSync.AtomicBool
	nextRefresh       *time.Timer
	refreshTokenMutex sync.Mutex
	status            int32
	shutdownWaiter    chan struct{}
	logger            logging.LoggerInterface
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
		authAPI:        authAPI,
		sseClient:      sse.NewStreamingClient(cfg, logger),
		statusTracker:  statusTracker,
		running:        gtSync.NewAtomicBool(false),
		feedback:       feedbackLoop,
		processor:      processor,
		parser:         parser,
		logger:         logger,
		shutdownWaiter: make(chan struct{}, 1),
	}, nil
}

// Start initiates the authentication flow and if successful initiates a connection
func (m *ManagerImpl) Start() error {
	if !atomic.CompareAndSwapInt32(&m.status, pushManagerStatusIdle, pushManagerStatusRunning) {
		return ErrAlreadyRunning
	}
	m.triggerConnectionFlow()
	return nil
}

// Stop method stops the sse client and it's status monitoring goroutine
func (m *ManagerImpl) Stop() error {
	if !atomic.CompareAndSwapInt32(&m.status, pushManagerStatusRunning, pushManagerStatusShuttingDown) {
		return ErrAlreadyRunning
	}
	m.statusTracker.NotifySSEShutdownExpected()
	m.withRefreshTokenLock(func() {
		if m.nextRefresh != nil {
			m.nextRefresh.Stop()
		}
	})
	m.StopWorkers()
	m.sseClient.StopStreaming()
	<-m.shutdownWaiter
	return nil
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
			m.logger.Error(fmt.Sprintf("Error authenticating: %s", err.Error()))
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

func (m *ManagerImpl) triggerConnectionFlow() {
	token, status := m.performAuthentication()
	if status != nil {
		atomic.StoreInt32(&m.status, pushManagerStatusIdle)
		m.feedback <- *status
		return
	}

	m.statusTracker.Reset()
	sseStatus := make(chan int, 100)

	tokenList, err := token.ChannelList()
	if err != nil {
		m.logger.Error("error parsing channel list: ", err)
		atomic.StoreInt32(&m.status, pushManagerStatusIdle)
		m.feedback <- StatusRetryableError
		return
	}

	m.sseClient.ConnectStreaming(token.Token, sseStatus, tokenList, m.eventHandler)

	go func() {
		defer func() {
			select {
			case m.shutdownWaiter <- struct{}{}:
			default:
			}
		}()
		defer atomic.StoreInt32(&m.status, pushManagerStatusIdle)
		for {
			message := <-sseStatus
			switch message {
			case sse.StatusFirstEventOk:
				when, err := token.CalculateNextTokenExpiration()
				if err != nil || when <= 0 {
					m.logger.Warning("Failed to calculate next token expiration time. Defaulting to 50 minutes")
					when = 50 * time.Minute
				}
				m.withRefreshTokenLock(func() {
					m.nextRefresh = time.AfterFunc(when, func() {
						m.Stop()
						m.triggerConnectionFlow()
					})
				})
				m.feedback <- StatusUp
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

func (m *ManagerImpl) withRefreshTokenLock(f func()) {
	m.refreshTokenMutex.Lock()
	defer m.refreshTokenMutex.Unlock()
	f()
}
