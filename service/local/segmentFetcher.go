package local

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

// FileSegmentFetcher struct fetches segments from a file
type FileSegmentFetcher struct {
	reader           Reader
	segmentDirectory string
	lastHash         map[string][]byte
	logger           logging.LoggerInterface
	mutex            sync.Mutex
}

// NewFileSegmentFetcher returns a new instance of LocalFileSegmentFetcher
func NewFileSegmentFetcher(segmentDirectory string, logger logging.LoggerInterface) service.SegmentFetcher {
	return &FileSegmentFetcher{
		segmentDirectory: segmentDirectory,
		lastHash:         make(map[string][]byte),
		logger:           logger,
		reader:           NewFileReader(),
		mutex:            sync.Mutex{},
	}
}

// parseSegmentJson returns a SegmentChangesDTO parsed from data
func (f *FileSegmentFetcher) parseSegmentJson(data string, segmentName string) (*dtos.SegmentChangesDTO, error) {
	var segmentChangesDto dtos.SegmentChangesDTO
	err := json.Unmarshal([]byte(data), &segmentChangesDto)
	if err != nil {
		f.logger.Error(fmt.Sprintf("error: %v", err))
		return nil, fmt.Errorf("couldn't parse segmentChange json for %s", segmentName)
	}
	return segmentSanitization(segmentChangesDto, segmentName)
}

// processSegmentJson returns a SegmentChangesDTO after apply the logic
func (s *FileSegmentFetcher) processSegmentJson(fileContents []byte, segmentName string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
	segmentChange, err := s.parseSegmentJson(string(fileContents), segmentName)
	if err != nil {
		return nil, err
	}
	// if the till is less than storage CN and different from the default till ignore the change
	if segmentChange.Till < changeNumber && segmentChange.Till != defaultTill {
		return nil, fmt.Errorf("ignoring change, the till is less than storage change number")
	}

	// create a json SegmentChangeDto with Added and Removed
	asJson, _ := json.Marshal(dtos.SegmentChangesDTO{Added: segmentChange.Added, Removed: segmentChange.Removed})
	currH := sha1.New()
	currH.Write(asJson)
	// calculate the json sha
	currSum := currH.Sum(nil)

	s.mutex.Lock()
	defer s.mutex.Unlock()
	lastHash, ok := s.lastHash[segmentName]
	//if sha exist and is equal to before sha, or if till is equal to default till returns the same segmentChange with till equals to storage CN
	if (ok && bytes.Equal(currSum, lastHash)) || segmentChange.Till == defaultTill {
		s.lastHash[segmentName] = currSum
		segmentChange.Since = changeNumber
		segmentChange.Till = changeNumber
		return segmentChange, nil
	}
	// In the last case, the sha is different and till upper or equal to storage CN
	s.lastHash[segmentName] = currSum
	segmentChange.Since = segmentChange.Till
	return segmentChange, nil
}

// Fetch parses the file and returns the appropriate structures
func (s *FileSegmentFetcher) Fetch(segmentName string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
	fileContents, err := s.reader.ReadFile(fmt.Sprintf("%v/%v.json", s.segmentDirectory, segmentName))
	if err != nil {
		s.logger.Error(fmt.Sprintf("could not find the segmentChange file for %s. error: %v", segmentName, err))
		return nil, err
	}
	return s.processSegmentJson(fileContents, segmentName, fetchOptions.ChangeNumber())
}
