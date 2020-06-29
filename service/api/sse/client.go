package sse

import (
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/sse"
)

const (
	streamingURL = "https://streaming.split.io/sse"
	occupancy    = "[?occupancy=metrics.publishers]control_pri,[?occupancy=metrics.publishers]control_sec"
	version      = "1.1"
	keepAlive    = 120
)

func getStreamingURL(cfg *conf.AdvancedConfig) string {
	if cfg != nil && cfg.StreamingServiceURL != "" {
		return cfg.StreamingServiceURL
	}
	return streamingURL
}

// StreamingClient struct
type StreamingClient struct {
	mutex           *sync.RWMutex
	sseClient       *sse.SSEClient
	sseStatus       chan int
	streamingStatus chan int
	running         bool
	logger          logging.LoggerInterface
}

// NewStreamingClient creates new SSE Client
func NewStreamingClient(cfg *conf.AdvancedConfig, streamingStatus chan int, logger logging.LoggerInterface) *StreamingClient {
	sseStatus := make(chan int, 1)
	sseClient, _ := sse.NewSSEClient(getStreamingURL(cfg), sseStatus, make(chan struct{}, 1), keepAlive, logger)

	return &StreamingClient{
		mutex:           &sync.RWMutex{},
		sseClient:       sseClient,
		sseStatus:       sseStatus,
		streamingStatus: streamingStatus,
		logger:          logger,
		running:         false,
	}
}

// ConnectStreaming connects to streaming
func (s *StreamingClient) ConnectStreaming(token string, channelList []string, handleIncomingMessage func(e map[string]interface{})) {
	params := make(map[string]string)
	params["channels"] = strings.Join(channelList, ",") + "," + occupancy
	params["accessToken"] = token
	params["v"] = version

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
					s.streamingStatus <- sse.OK
				case sse.ErrorConnectToStreaming:
					s.logger.Error("Error connecting to streaming")
					s.streamingStatus <- sse.ErrorConnectToStreaming
				case sse.ErrorKeepAlive:
					s.logger.Error("Connection timed out")
					s.streamingStatus <- sse.ErrorKeepAlive
				case sse.ErrorOnClientCreation:
					s.logger.Error("Could not create client for streaming")
					s.streamingStatus <- sse.ErrorOnClientCreation
				case sse.ErrorReadingStream:
					s.logger.Error("Error reading streaming buffer")
					s.streamingStatus <- sse.ErrorReadingStream
				case sse.ErrorRequestPerformed:
					s.logger.Error("Error performing request when connect to stream service")
					s.streamingStatus <- sse.ErrorRequestPerformed
				default:
					s.logger.Error("Unexpected error occured with streaming")
					s.streamingStatus <- sse.ErrorUnexpected
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
