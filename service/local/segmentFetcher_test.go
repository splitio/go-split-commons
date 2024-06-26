package local

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/service/local/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLocalSegmentFetcherJson(t *testing.T) {

	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	res, err := fetcher.Fetch("segment_mock", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 1489542661161 || res.Till != 1489542661161 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if res.Name != "employees" {
		t.Error("should name be employees. is: ", res.Name)
	}

	if res.Added[0] != "user_for_testing_do_no_erase" {
		t.Error("DTO mal formed")
	}

	if len(res.Removed) != 0 {
		t.Error("DTO mal formed")
	}
}

func TestInvalidTill(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	res, err := fetcher.Fetch("segmentTillInvalid", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if err != nil {
		t.Error("should not fail.")
	}
	if res.Till != -1 {
		t.Error("till should be -1. Got: ", res.Till)
	}
}

func TestFetchSomeSegments(t *testing.T) {
	fetches := 0
	mockedFetchers := FileSegmentFetcher{
		reader: mocks.MockReader{
			ReadFileCall: func(filename string) ([]byte, error) {
				if filename != "/test_1.json" {
					t.Error("Cannot construct path")
				}
				switch fetches {
				case 0:
					segmentChange := dtos.SegmentChangesDTO{
						Name:    "case_0",
						Added:   []string{"user-1"},
						Removed: make([]string, 0),
						Till:    -1,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 1:
					segmentChange := dtos.SegmentChangesDTO{
						Name:    "case_1",
						Added:   []string{"user-1"},
						Removed: []string{"user-2"},
						Till:    -1,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 2:
					segmentChange := dtos.SegmentChangesDTO{
						Name:    "case_2",
						Added:   []string{"user-1"},
						Removed: []string{"user-2"},
						Till:    2323,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 3:
					segmentChange := dtos.SegmentChangesDTO{
						Name:    "case_3",
						Added:   []string{"user-1", "user-3"},
						Removed: []string{"user-2"},
						Till:    2323,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 4:
					segmentChange := dtos.SegmentChangesDTO{
						Name:    "case_4",
						Added:   []string{"user-1", "user-3"},
						Removed: []string{"user-2"},
						Till:    445345,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 5:
					segmentChange := dtos.SegmentChangesDTO{
						Name:    "case_5",
						Added:   []string{"user-1"},
						Removed: []string{"user-2", "user-3"},
						Till:    -1,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				}
				segmentChange := dtos.SegmentChangesDTO{
					Name:    "case_6",
					Added:   make([]string, 0),
					Removed: make([]string, 0),
					Till:    -1,
					Since:   -1,
				}
				return []byte(fmt.Sprintf("%v", segmentChange)), nil
			},
		},
		logger:   &logging.Logger{},
		lastHash: make(map[string][]byte),
	}

	// 0) The CN from storage is -1, till and since are -1, and sha doesn't exist in the hash. It's going to return a segment change with updates.
	segmentChange, _ := mockedFetchers.Fetch("test_1", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if segmentChange.Since != -1 || segmentChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if !contains(segmentChange.Added, "user-1") {
		t.Error("DTO mal formed")
	}
	if len(segmentChange.Removed) != 0 {
		t.Error("DTO mal formed")
	}

	fetches++
	// 1) The CN from storage is -1, till and since are -1, and sha is different than before. It's going to return a segment change with updates.
	segmentChange, _ = mockedFetchers.Fetch("test_1", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if segmentChange.Since != -1 || segmentChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if !contains(segmentChange.Added, "user-1") {
		t.Error("DTO mal formed")
	}
	if !contains(segmentChange.Removed, "user-2") {
		t.Error("DTO mal formed")
	}

	fetches++
	// 2) The CN from storage is -1, till is 2323, and since is -1, and sha is the same as before. It's going to return a segment change with the same data.
	segmentChange, _ = mockedFetchers.Fetch("test_1", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if segmentChange.Since != -1 || segmentChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if !contains(segmentChange.Added, "user-1") {
		t.Error("DTO mal formed")
	}
	if !contains(segmentChange.Removed, "user-2") {
		t.Error("DTO mal formed")
	}

	fetches++
	// 3) The CN from storage is -1, till is 2323, and since is -1, sha is different than before. It's going to return a segment change with updates.
	segmentChange, _ = mockedFetchers.Fetch("test_1", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if segmentChange.Since != 2323 || segmentChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if !contains(segmentChange.Added, "user-1") || !contains(segmentChange.Added, "user-3") {
		t.Error("DTO mal formed")
	}
	if !contains(segmentChange.Removed, "user-2") {
		t.Error("DTO mal formed")
	}

	fetches++
	// 4) The CN from storage is 2323, till is 445345, and since is -1, and sha is the same as before. It's going to return a segment change with same data.
	segmentChange, _ = mockedFetchers.Fetch("test_1", service.MakeSegmentRequestParams().WithChangeNumber(2323))
	if segmentChange.Since != 2323 || segmentChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if !contains(segmentChange.Added, "user-1") || !contains(segmentChange.Added, "user-3") {
		t.Error("DTO mal formed")
	}
	if !contains(segmentChange.Removed, "user-2") {
		t.Error("DTO mal formed")
	}

	fetches++
	// 5) The CN from storage is 2323, till and since are -1, and sha is different than before. It's going to return a segment change with updates.
	segmentChange, _ = mockedFetchers.Fetch("test_1", service.MakeSegmentRequestParams().WithChangeNumber(2323))
	if segmentChange.Since != 2323 || segmentChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if !contains(segmentChange.Added, "user-1") {
		t.Error("DTO mal formed")
	}

	if !contains(segmentChange.Removed, "user-2") || !contains(segmentChange.Removed, "user-3") {
		t.Error("DTO mal formed")
	}
}

func TestSegmentWithoutName(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	_, err := fetcher.Fetch("segmentWithoutName", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if err == nil {
		t.Error("fetching should fail.")
	}
}

func TestSegmentSanitization(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	res, err := fetcher.Fetch("segmentSanitization", service.MakeSegmentRequestParams().WithChangeNumber(-1))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}
	if res.Name != "segmentSanitization" {
		t.Error("the segment name should be segmentSanitization. Got: ", res.Name)
	}
	if len(res.Added) != 2 {
		t.Error("added size sould be 2. Got: ", res.Added)
	}
	if len(res.Removed) != 1 {
		t.Error("removed size sould be =1. Got: ", res.Removed)
	}
	if res.Since != -1 {
		t.Error("since should be -1. Got: ", res.Since)
	}
	if res.Till != -1 {
		t.Error("till should be -1. Got: ", res.Till)
	}
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}
