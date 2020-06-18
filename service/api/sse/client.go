package sse

import (
	"strings"
	"sync/atomic"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/sse"
)

const streamingURL = "https://streaming.split.io/sse"

func getStreamingURL(cfg *conf.AdvancedConfig) string {
	if cfg != nil && cfg.StreamingServiceURL != "" {
		return cfg.StreamingServiceURL
	}
	return streamingURL
}

// StreamingClient struct
type StreamingClient struct {
	sseClient *sse.SSEClient
	sseError  chan error
	isRunning int64
	logger    logging.LoggerInterface
}

// NewStreamingClient creates new SSE Client
func NewStreamingClient(cfg *conf.AdvancedConfig, sseReady chan struct{}, sseError chan error, logger logging.LoggerInterface) *StreamingClient {
	return &StreamingClient{
		sseClient: sse.NewSSEClient(getStreamingURL(cfg), sseReady, logger),
		sseError:  sseError,
		logger:    logger,
	}
}

// ConnectStreaming connects to streaming
func (s *StreamingClient) ConnectStreaming(token string, channelList []string, handleIncommingMessage func(e map[string]interface{})) {
	params := make(map[string]string)
	params["channels"] = strings.Join(channelList, ",")
	params["accessToken"] = token
	params["v"] = "1.1"

	err := s.sseClient.Do(params, handleIncommingMessage)
	if err != nil {
		atomic.StoreInt64(&s.isRunning, 0)
		s.sseError <- err
		return
	}
	atomic.StoreInt64(&s.isRunning, 1)
}

// StopStreaming stops streaming
func (s *StreamingClient) StopStreaming() {
	s.sseClient.Shutdown()
	atomic.StoreInt64(&s.isRunning, 0)
}

// IsRunning returns true if it's running
func (s *StreamingClient) IsRunning() bool {
	return atomic.LoadInt64(&s.isRunning) == 1
}
