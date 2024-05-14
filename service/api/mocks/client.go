package mocks

import "github.com/splitio/go-split-commons/v6/service"

// ClientMock mocks client
type ClientMock struct {
	GetCall  func(endpoint string, fetchOptions service.RequestParams) ([]byte, error)
	PostCall func(endpoint string, body []byte, headers map[string]string) error
}

// Get mocks Get
func (c ClientMock) Get(endpoint string, fetchOptions service.RequestParams) ([]byte, error) {
	return c.GetCall(endpoint, fetchOptions)
}

// Post mocks Post
func (c ClientMock) Post(service string, body []byte, headers map[string]string) error {
	return c.PostCall(service, body, headers)
}
