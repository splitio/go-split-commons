package local

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

// FileSegmentFetcher struct fetches segments from a file
type FileSegmentFetcher struct {
	segmentDirectory string
	lastHash         []byte
	logger           logging.LoggerInterface
}

// NewFileSegmentFetcher returns a new instance of LocalFileSegmentFetcher
func NewFileSegmentFetcher(segmentDirectory string, logger logging.LoggerInterface) service.SegmentFetcher {
	return &FileSegmentFetcher{
		segmentDirectory: segmentDirectory,
		logger:           logger,
	}
}

func (f *FileSegmentFetcher) parseSegmentJson(data string) *dtos.SegmentChangesDTO {
	var segmentChangesDto dtos.SegmentChangesDTO
	err := json.Unmarshal([]byte(data), &segmentChangesDto)
	if err != nil {
		f.logger.Error(fmt.Sprintf("error: %v", err))
		return &segmentChangesDto
	}
	return &segmentChangesDto
}

// Fetch parses the file and returns the appropriate structures
func (s *FileSegmentFetcher) Fetch(segmentName string, changeNumber int64, _ *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
	fileContents, err := ioutil.ReadFile(fmt.Sprintf("%v/%v.json", s.segmentDirectory, segmentName))
	if err != nil {
		return nil, err
	}
	data := string(fileContents)
	segmentChange := s.parseSegmentJson(data)
	segmentChange.Since = segmentChange.Till
	return segmentChange, nil
}
