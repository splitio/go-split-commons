package sse

import (
	"errors"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/sse"
)

const (
	streamingURL = "https://streaming.split.io/sse"
	occupancy    = "[?occupancy=metrics.publishers]control_pri,[?occupancy=metrics.publishers]control_sec"
)

func getStreamingURL(cfg *conf.AdvancedConfig) string {
	if cfg != nil && cfg.StreamingServiceURL != "" {
		return cfg.StreamingServiceURL
	}
	return streamingURL
}

// StreamingClient struct
type StreamingClient struct {
	mutex     *sync.RWMutex
	sseClient *sse.SSEClient
	sseStatus chan int
	sseReady  chan struct{}
	sseError  chan error
	running   bool
	logger    logging.LoggerInterface
}

// NewStreamingClient creates new SSE Client
func NewStreamingClient(cfg *conf.AdvancedConfig, sseReady chan struct{}, sseError chan error, logger logging.LoggerInterface) *StreamingClient {
	status := make(chan int, 1)
	sseClient, _ := sse.NewSSEClient(getStreamingURL(cfg), status, make(chan struct{}, 1), logger)

	return &StreamingClient{
		mutex:     &sync.RWMutex{},
		sseClient: sseClient,
		sseStatus: status,
		sseReady:  sseReady,
		sseError:  sseError,
		logger:    logger,
		running:   false,
	}
}

// ConnectStreaming connects to streaming
func (s *StreamingClient) ConnectStreaming(token string, channelList []string, handleIncomingMessage func(e map[string]interface{})) {
	params := make(map[string]string)
	params["channels"] = strings.Join(channelList, ",") + "," + occupancy
	params["accessToken"] = token
	params["v"] = "1.1"

	go s.sseClient.Do(params, handleIncomingMessage)
	go func() {
		for {
			select {
			case status := <-s.sseStatus:
				switch status {
				case sse.OK:
					s.mutex.Lock()
					s.running = true
					s.mutex.Unlock()
					s.sseReady <- struct{}{}
				default:
					s.sseError <- errors.New("Some error occurred connecting to streaming")
				}
			}
		}
	}()
}

// StopStreaming stops streaming
func (s *StreamingClient) StopStreaming() {
	defer s.mutex.Unlock()
	s.sseClient.Shutdown()
	s.mutex.Lock()
	s.running = false
}

// IsRunning returns true if it's running
func (s *StreamingClient) IsRunning() bool {
	defer s.mutex.RUnlock()
	s.mutex.RLock()
	return s.running
}
