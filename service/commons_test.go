package service

import (
	"net/http"
	"testing"

	"github.com/splitio/go-toolkit/v5/common"
)

func TestSplitFetchOptions(t *testing.T) {
	fetchOptions := MakeSplitFetchOptions(common.StringRef("v1")).WithChangeNumber(123456).WithFlagSetsFilter("filter").WithTill(*common.Int64Ref(123))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	if req.Header.Get(cacheControl) != cacheControlNoCache {
		t.Error("Cache control header not set")
	}
	if req.URL.Query().Get(since) != "123456" {
		t.Error("Change number not set")
	}
	if req.URL.Query().Get(spec) != "v1" {
		t.Error("Spec version not set")
	}
	if req.URL.Query().Get(sets) != "filter" {
		t.Error("Flag sets filter not set")
	}
	if req.URL.Query().Get(till) != "123" {
		t.Error("Till not set")
	}
	if req.URL.String() != "test?s=v1&since=123456&sets=filter&till=123" {
		t.Error("Query params not set correctly, expected: test?s=v1&since=123456&sets=filter&till=123, got:", req.URL.String())
	}
}

func TestSegmentFetchOptions(t *testing.T) {
	fetchOptions := MakeSegmentFetchOptions(common.StringRef("v1")).WithChangeNumber(123456).WithTill(*common.Int64Ref(123))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	if req.Header.Get(cacheControl) != cacheControlNoCache {
		t.Error("Cache control header not set")
	}

	if req.URL.Query().Get(since) != "123456" {
		t.Error("Change number not set")
	}
	if req.URL.Query().Get(spec) != "v1" {
		t.Error("Spec version not set")
	}
	if req.URL.Query().Get(till) != "123" {
		t.Error("Till not set")
	}

	if req.URL.String() != "test?s=v1&since=123456&till=123" {
		t.Error("Query params not set correctly, expected: test?s=v1&since=123456&till=123, got:", req.URL.String())
	}
}

func TestAuthFetchOptions(t *testing.T) {
	fetchOptions := MakeAuthFetchOptions(common.StringRef("v1"))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	if req.Header.Get(cacheControl) != cacheControlNoCache {
		t.Error("Cache control header not set")
	}
	if req.URL.Query().Get(spec) != "v1" {
		t.Error("Spec version not set")
	}
	if req.URL.String() != "test?s=v1" {
		t.Error("Query params not set correctly, expected: test?s=v1, got:", req.URL.String())
	}
}
