package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service"
)

type MockLargeSegmentFetcher struct {
	FetchCall        func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error)
	DownloadFileCall func(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error)
}

func (m MockLargeSegmentFetcher) Fetch(name string, fetchOptions *service.SegmentRequestParams) (*dtos.LargeSegmentRFDResponseDTO, error) {
	return m.FetchCall(name, fetchOptions)
}

func (m MockLargeSegmentFetcher) DownloadFile(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*dtos.LargeSegment, error) {
	return m.DownloadFileCall(name, lsRFDResponseDTO)
}
