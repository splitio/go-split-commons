package service

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/splitio/go-toolkit/v5/common"
)

const (
	cacheControl        = "Cache-Control"
	cacheControlNoCache = "no-cache"
	sets                = "sets"
	since               = "since"
	spec                = "s"
	till                = "till"
)

type queryParamater struct {
	key   string
	value string
}

type RequestParams interface {
	Apply(request *http.Request) error
}

type BaseRequestParams struct {
	CacheControlHeaders bool
}

type FlagRequestParams struct {
	BaseRequestParams
	ChangeNumber   int64
	FlagSetsFilter string
	SpecVersion    *string
	Till           *int64
}

func MakeFlagRequestParams() *FlagRequestParams {
	return &FlagRequestParams{
		BaseRequestParams: BaseRequestParams{
			CacheControlHeaders: true,
		},
	}
}

func (s *FlagRequestParams) WithCacheControl(cacheControl bool) *FlagRequestParams {
	s.CacheControlHeaders = cacheControl
	return s
}

func (s *FlagRequestParams) WithChangeNumber(changeNumber int64) *FlagRequestParams {
	s.ChangeNumber = changeNumber
	return s
}

func (s *FlagRequestParams) WithFlagSetsFilter(flagSetsFilter string) *FlagRequestParams {
	s.FlagSetsFilter = flagSetsFilter
	return s
}

func (s *FlagRequestParams) WithSpecVersion(specVersion *string) *FlagRequestParams {
	s.SpecVersion = specVersion
	return s
}

func (s *FlagRequestParams) WithTill(till int64) *FlagRequestParams {
	s.Till = common.Int64Ref(till)
	return s
}

func (s *FlagRequestParams) Apply(request *http.Request) error {
	if s.CacheControlHeaders {
		request.Header.Set(cacheControl, cacheControlNoCache)
	}

	queryParameters := []queryParamater{}
	if s.SpecVersion != nil {
		queryParameters = append(queryParameters, queryParamater{key: spec, value: common.StringFromRef(s.SpecVersion)})
	}
	queryParameters = append(queryParameters, queryParamater{key: since, value: strconv.FormatInt(s.ChangeNumber, 10)})
	if len(s.FlagSetsFilter) > 0 {
		queryParameters = append(queryParameters, queryParamater{key: sets, value: s.FlagSetsFilter})
	}
	if s.Till != nil {
		queryParameters = append(queryParameters, queryParamater{key: till, value: strconv.FormatInt(*s.Till, 10)})
	}

	request.URL.RawQuery = encode(queryParameters)
	return nil
}

type SegmentRequestParams struct {
	BaseRequestParams
	ChangeNumber int64
	Till         *int64
}

func MakeSegmentRequestParams() *SegmentRequestParams {
	return &SegmentRequestParams{
		BaseRequestParams: BaseRequestParams{
			CacheControlHeaders: true,
		},
	}
}

func (s *SegmentRequestParams) WithCacheControl(cacheControl bool) *SegmentRequestParams {
	s.CacheControlHeaders = cacheControl
	return s
}

func (s *SegmentRequestParams) WithChangeNumber(changeNumber int64) *SegmentRequestParams {
	s.ChangeNumber = changeNumber
	return s
}

func (s *SegmentRequestParams) WithTill(till int64) *SegmentRequestParams {
	s.Till = common.Int64Ref(till)
	return s
}

func (s *SegmentRequestParams) Apply(request *http.Request) error {
	if s.CacheControlHeaders {
		request.Header.Set(cacheControl, cacheControlNoCache)
	}

	queryParameters := []queryParamater{}
	queryParameters = append(queryParameters, queryParamater{key: since, value: strconv.FormatInt(s.ChangeNumber, 10)})
	if s.Till != nil {
		queryParameters = append(queryParameters, queryParamater{key: till, value: strconv.FormatInt(*s.Till, 10)})
	}

	request.URL.RawQuery = encode(queryParameters)
	return nil
}

type AuthRequestParams struct {
	BaseRequestParams
	SpecVersion *string
}

func MakeAuthRequestParams(specVersion *string) *AuthRequestParams {
	return &AuthRequestParams{
		BaseRequestParams: BaseRequestParams{
			CacheControlHeaders: true,
		},
		SpecVersion: specVersion,
	}
}

func (s *AuthRequestParams) WithCacheControl(cacheControl bool) *AuthRequestParams {
	s.CacheControlHeaders = cacheControl
	return s
}

func (s *AuthRequestParams) Apply(request *http.Request) error {
	if s.CacheControlHeaders {
		request.Header.Set(cacheControl, cacheControlNoCache)
	}

	queryParams := request.URL.Query()
	if s.SpecVersion != nil {
		queryParams.Add(spec, common.StringFromRef(s.SpecVersion))
	}

	request.URL.RawQuery = queryParams.Encode()
	return nil
}

func encode(v []queryParamater) string {
	if v == nil {
		return ""
	}
	var buf strings.Builder
	for _, k := range v {
		keyEscaped := url.QueryEscape(k.key)
		if buf.Len() > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(keyEscaped)
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(k.value))
	}
	return buf.String()
}
