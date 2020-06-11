package sse

import (
	"strings"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/sse"
)

const streamingURL = "https://streaming.split.io/sse"

func getStreamingURL(cfg *conf.AdvancedConfig) string {
	if cfg != nil && cfg.StreamingURL != "" {
		return cfg.StreamingURL
	}
	return streamingURL
}

// StreamingClient struct
type StreamingClient struct {
	sseClient *sse.SSEClient
	logger    logging.LoggerInterface
}

// NewStreamingClient creates new SSE Client
func NewStreamingClient(cfg *conf.AdvancedConfig, logger logging.LoggerInterface) *StreamingClient {
	sseReady := make(chan struct{}, 1)
	return &StreamingClient{
		sseClient: sse.NewSSEClient(getStreamingURL(cfg), sseReady, logger),
		logger:    logger,
	}
}

// ConnectStreaming connects to streaming
func (s *StreamingClient) ConnectStreaming(token string, channelList []string, handleIncommingMessage func(e map[string]interface{})) error {
	params := make(map[string]string)
	params["channels"] = strings.Join(channelList, ",")
	params["accessToken"] = token
	params["v"] = "1.1"

	err := s.sseClient.Do(params, handleIncommingMessage)
	if err != nil {
		return err
	}
	return nil
}

// StopStreaming stops streaming
func (s *StreamingClient) StopStreaming() {
	s.sseClient.Shutdown()
}
