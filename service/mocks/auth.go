package mocks

import "github.com/splitio/go-split-commons/v4/dtos"

// MockAuthClient mocked implementation of auth
type MockAuthClient struct {
	AuthenticateCall func() (*dtos.Token, error)
}

// Authenticate mock
func (m MockAuthClient) Authenticate() (*dtos.Token, error) {
	return m.AuthenticateCall()
}
