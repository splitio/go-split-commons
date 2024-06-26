package service

import (
	"net/http"
	"testing"

	"github.com/splitio/go-split-commons/v6/service/api/specs"
	"github.com/splitio/go-toolkit/v5/common"
)

func TestSplitFetchOptions(t *testing.T) {
	fetchOptions := MakeFlagRequestParams().WithChangeNumber(123456).WithFlagSetsFilter("filter").WithTill(*common.Int64Ref(123)).WithSpecVersion(common.StringRef(specs.FLAG_V1_1))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	if req.Header.Get(cacheControl) != cacheControlNoCache {
		t.Error("Cache control header not set")
	}
	if req.URL.Query().Get(since) != "123456" {
		t.Error("Change number not set")
	}
	if req.URL.Query().Get(spec) != specs.FLAG_V1_1 {
		t.Error("Spec version not set")
	}
	if req.URL.Query().Get(sets) != "filter" {
		t.Error("Flag sets filter not set")
	}
	if req.URL.Query().Get(till) != "123" {
		t.Error("Till not set")
	}
	if req.URL.String() != "test?s=1.1&since=123456&sets=filter&till=123" {
		t.Error("Query params not set correctly, expected: test?s=v1&since=123456&sets=filter&till=123, got:", req.URL.String())
	}
}

func TestSegmentRequestParams(t *testing.T) {
	fetchOptions := MakeSegmentRequestParams().WithChangeNumber(123456).WithTill(*common.Int64Ref(123))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	if req.Header.Get(cacheControl) != cacheControlNoCache {
		t.Error("Cache control header not set")
	}

	if req.URL.Query().Get(since) != "123456" {
		t.Error("Change number not set")
	}
	if req.URL.Query().Get(till) != "123" {
		t.Error("Till not set")
	}

	if req.URL.String() != "test?since=123456&till=123" {
		t.Error("Query params not set correctly, expected: test?s=v1&since=123456&till=123, got:", req.URL.String())
	}
}

func TestAuthRequestParams(t *testing.T) {
	fetchOptions := MakeAuthRequestParams(common.StringRef(specs.FLAG_V1_1))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	if req.Header.Get(cacheControl) != cacheControlNoCache {
		t.Error("Cache control header not set")
	}
	if req.URL.Query().Get(spec) != specs.FLAG_V1_1 {
		t.Error("Spec version not set")
	}
	if req.URL.String() != "test?s=1.1" {
		t.Error("Query params not set correctly, expected: test?s=v1, got:", req.URL.String())
	}
}

func TestOverrideCacheControl(t *testing.T) {
	flagParams := MakeFlagRequestParams().WithCacheControl(false)
	req, _ := http.NewRequest("GET", "test", nil)
	flagParams.Apply(req)

	if req.Header.Get(cacheControl) != "" {
		t.Error("Cache control header should not be set")
	}

	segmentParams := MakeSegmentRequestParams().WithCacheControl(false)
	req, _ = http.NewRequest("GET", "test", nil)
	segmentParams.Apply(req)

	if req.Header.Get(cacheControl) != "" {
		t.Error("Cache control header should not be set")
	}

	authParams := MakeAuthRequestParams(nil).WithCacheControl(false)
	req, _ = http.NewRequest("GET", "test", nil)
	authParams.Apply(req)

	if req.Header.Get(cacheControl) != "" {
		t.Error("Cache control header should not be set")
	}
}
