package local

import (
	"testing"

	"github.com/splitio/go-split-commons/v4/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLocalSegmentFetcherJson(t *testing.T) {

	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	res, err := fetcher.Fetch("segment_mock", -1, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 1489542661161 || res.Till != 1489542661162 {
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

	res, err := fetcher.Fetch("segmentTillInvalid", -1, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != -1 || res.Till != -1 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if res.Name != "" {
		t.Error("should name be empty. is: ", res.Name)
	}

	if len(res.Added) != 0 {
		t.Error("DTO mal formed")
	}

	if len(res.Removed) != 0 {
		t.Error("DTO mal formed")
	}
}
