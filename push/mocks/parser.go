package mocks

import (
	"github.com/splitio/go-split-commons/v5/service/api/sse"
)

type NotificationParserMock struct {
	ParseAndForwardCall func(m sse.IncomingMessage) (*int64, error)
}

func (n *NotificationParserMock) ParseAndForward(m sse.IncomingMessage) (*int64, error) {
	return n.ParseAndForwardCall(m)
}
