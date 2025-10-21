package service

import (
	"net/http"
	"testing"

	"github.com/splitio/go-split-commons/v8/service/api/specs"

	"github.com/splitio/go-toolkit/v5/common"

	"github.com/stretchr/testify/assert"
)

func TestSplitFetchOptions(t *testing.T) {
	fetchOptions := MakeFlagRequestParams().WithChangeNumber(123456).WithFlagSetsFilter("filter").WithTill(*common.Int64Ref(123)).WithSpecVersion(common.StringRef(specs.FLAG_V1_1)).WithChangeNumberRB(123456)

	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	assert.Equal(t, cacheControlNoCache, req.Header.Get(cacheControl))
	assert.Equal(t, "123456", req.URL.Query().Get(since))
	assert.Equal(t, specs.FLAG_V1_1, req.URL.Query().Get(spec))
	assert.Equal(t, "filter", req.URL.Query().Get(sets))
	assert.Equal(t, "123", req.URL.Query().Get(till))
	assert.Equal(t, "123456", req.URL.Query().Get(rbSince))
	assert.Equal(t, "test?s=1.1&since=123456&rbSince=123456&sets=filter&till=123", req.URL.String())
	assert.Equal(t, "1.1", fetchOptions.SpecVersion())

	fetchOptions = MakeFlagRequestParams()
	assert.Equal(t, "", fetchOptions.SpecVersion())
}

func TestSegmentRequestParams(t *testing.T) {
	fetchOptions := MakeSegmentRequestParams().WithChangeNumber(123456).WithTill(*common.Int64Ref(123))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	assert.Equal(t, cacheControlNoCache, req.Header.Get(cacheControl))
	assert.Equal(t, "123456", req.URL.Query().Get(since))
	assert.Equal(t, "123", req.URL.Query().Get(till))
	assert.Equal(t, "test?since=123456&till=123", req.URL.String())
}

func TestAuthRequestParams(t *testing.T) {
	fetchOptions := MakeAuthRequestParams(common.StringRef(specs.FLAG_V1_1))
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)

	assert.Equal(t, cacheControlNoCache, req.Header.Get(cacheControl))
	assert.Equal(t, "1.1", req.URL.Query().Get(spec))
	assert.Equal(t, "test?s=1.1", req.URL.String())
}

func TestOverrideCacheControl(t *testing.T) {
	flagParams := MakeFlagRequestParams().WithCacheControl(false)
	req, _ := http.NewRequest("GET", "test", nil)
	flagParams.Apply(req)
	assert.Equal(t, "", req.Header.Get(cacheControl))

	segmentParams := MakeSegmentRequestParams().WithCacheControl(false)
	req, _ = http.NewRequest("GET", "test", nil)
	segmentParams.Apply(req)
	assert.Equal(t, "", req.Header.Get(cacheControl))

	authParams := MakeAuthRequestParams(nil).WithCacheControl(false)
	req, _ = http.NewRequest("GET", "test", nil)
	authParams.Apply(req)
	assert.Equal(t, "", req.Header.Get(cacheControl))
}
