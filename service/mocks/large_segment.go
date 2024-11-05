package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service"
)

type MockLargeSegmentFetcher struct {
	RequestForExportCall func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.RfeDTO, error)
	FetchCall            func(rfe dtos.RfeDTO) (*dtos.LargeSegmentDTO, error)
}

func (m MockLargeSegmentFetcher) RequestForExport(name string, fetchOptions *service.SegmentRequestParams) (*dtos.RfeDTO, error) {
	return m.RequestForExportCall(name, fetchOptions)
}

func (m MockLargeSegmentFetcher) Fetch(rfe dtos.RfeDTO) (*dtos.LargeSegmentDTO, error) {
	return m.FetchCall(rfe)
}
