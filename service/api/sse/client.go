package sse

import (
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/v10/conf"
	"github.com/splitio/go-split-commons/v10/dtos"
	"github.com/splitio/go-split-commons/v10/service/api"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/sse"
	"github.com/splitio/go-toolkit/v5/struct/traits/lifecycle"
)

const (
	version   = "1.1"
	keepAlive = 70
)

// StreamingClient interface
type StreamingClient interface {
	ConnectStreaming(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(IncomingMessage))
	StopStreaming()
	IsRunning() bool
}

// StreamingClientImpl struct
type StreamingClientImpl struct {
	sseClient *sse.Client
	logger    logging.LoggerInterface
	lifecycle lifecycle.Manager
	metadata  dtos.Metadata
	clientKey *string

	statusMu sync.Mutex
	status   chan int // status channel of the connection currently being established
}

// Status constants
const (
	StatusConnectionFailed = iota
	StatusUnderlyingClientInUse
	StatusFirstEventOk // the streaming connection has been established (reported before any event is read)
	StatusDisconnected
)

// IncomingMessage is an alias of sse.RawEvent
type IncomingMessage = sse.RawEvent

// NewStreamingClient creates new SSE Client
func NewStreamingClient(cfg *conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata, clientKey *string) *StreamingClientImpl {
	var opts []sse.Option
	if cfg.StreamingForceHTTP1 {
		// Streaming (SSE) is a single long-lived stream; HTTP/2 provides no benefit here
		// and its client stack emits noisy "protocol error: received DATA after
		// END_STREAM" logs. Force HTTP/1.1 for this connection only.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = http.ProxyFromEnvironment
		transport.ForceAttemptHTTP2 = false
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{} // non-nil empty => no h2
		opts = append(opts, sse.WithCustomTransport(transport))
	}
	client := &StreamingClientImpl{
		logger:    logger,
		metadata:  metadata,
		clientKey: clientKey,
	}
	opts = append(opts, sse.WithOnConnected(client.notifyConnected))
	client.sseClient, _ = sse.NewClient(cfg.StreamingServiceURL, keepAlive, cfg.HTTPTimeout, logger, opts...)
	client.lifecycle.Setup()
	return client
}

// notifyConnected reports the established connection to the status channel of the ongoing ConnectStreaming call
func (s *StreamingClientImpl) notifyConnected() {
	s.statusMu.Lock()
	status := s.status
	s.statusMu.Unlock()
	if status != nil {
		status <- StatusFirstEventOk
	}
}

// ConnectStreaming connects to streaming
func (s *StreamingClientImpl) ConnectStreaming(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(IncomingMessage)) {

	if !s.lifecycle.BeginInitialization() {
		s.logger.Info("Connection is already in process/running. Ignoring")
		return
	}

	s.statusMu.Lock()
	s.status = streamingStatus
	s.statusMu.Unlock()

	params := make(map[string]string)
	params["channels"] = strings.Join(append(channelList), ",")
	params["accessToken"] = token
	params["v"] = version

	go func() {
		defer s.lifecycle.ShutdownComplete()
		if !s.lifecycle.InitializationComplete() {
			return
		}
		out := s.sseClient.Do(params, api.AddMetadataToHeaders(s.metadata, nil, s.clientKey), handleIncomingMessage)

		if out == nil { // all good
			streamingStatus <- StatusDisconnected
			return
		}

		// Something didn'g go as expected
		s.lifecycle.AbnormalShutdown()

		asConnectionFailedError := &sse.ErrConnectionFailed{}
		if errors.As(out, &asConnectionFailedError) {
			streamingStatus <- StatusConnectionFailed
			return
		}

		switch out {
		case sse.ErrNotIdle:
			// If this happens we have a bug
			streamingStatus <- StatusUnderlyingClientInUse
		case sse.ErrReadingStream:
			streamingStatus <- StatusDisconnected
		case sse.ErrTimeout:
			streamingStatus <- StatusDisconnected
		default:
		}
	}()
}

// StopStreaming stops streaming
func (s *StreamingClientImpl) StopStreaming() {
	if !s.lifecycle.BeginShutdown() {
		s.logger.Info("SSE client wrapper not running. Ignoring")
		return
	}
	s.sseClient.Shutdown(true)
	s.lifecycle.AwaitShutdownComplete()
	s.logger.Info("Stopped streaming")
}

// IsRunning returns true if the client is running
func (s *StreamingClientImpl) IsRunning() bool {
	return s.lifecycle.IsRunning()
}
