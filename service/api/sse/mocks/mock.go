package mocks

import "github.com/splitio/go-split-commons/service/api/sse"

type StreamingClientMock struct {
	ConnectStreamingCall func(token string, streamingStatus chan int, channelList []string, handleIncomingMessage func(sse.IncomingMessage))
	StopStreamingCall    func()
	IsRunningCall        func() bool
}

func (s *StreamingClientMock) ConnectStreaming(
	token string,
	streamingStatus chan int,
	channelList []string,
	handleIncomingMessage func(sse.IncomingMessage),
) {
	s.ConnectStreamingCall(token, streamingStatus, channelList, handleIncomingMessage)
}

func (s *StreamingClientMock) StopStreaming() {
	s.StopStreamingCall()
}

func (s *StreamingClientMock) IsRunning() bool {
	return s.IsRunning()
}
