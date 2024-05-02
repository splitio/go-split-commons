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

type baseRequestParams struct {
	cacheControlHeaders bool
}

type FlagRequestParams struct {
	baseRequestParams
	changeNumber   int64
	flagSetsFilter string
	specVersion    *string
	till           *int64
}

func MakeFlagRequestParams() *FlagRequestParams {
	return &FlagRequestParams{
		baseRequestParams: baseRequestParams{
			cacheControlHeaders: true,
		},
	}
}

func (s *FlagRequestParams) WithCacheControl(cacheControl bool) *FlagRequestParams {
	s.cacheControlHeaders = cacheControl
	return s
}

func (s *FlagRequestParams) WithChangeNumber(changeNumber int64) *FlagRequestParams {
	s.changeNumber = changeNumber
	return s
}

func (s *FlagRequestParams) WithFlagSetsFilter(flagSetsFilter string) *FlagRequestParams {
	s.flagSetsFilter = flagSetsFilter
	return s
}

func (s *FlagRequestParams) WithSpecVersion(specVersion *string) *FlagRequestParams {
	s.specVersion = specVersion
	return s
}

func (s *FlagRequestParams) WithTill(till int64) *FlagRequestParams {
	s.till = common.Int64Ref(till)
	return s
}

func (s *FlagRequestParams) ChangeNumber() int64 {
	return s.changeNumber
}

func (s *FlagRequestParams) Apply(request *http.Request) error {
	if s.cacheControlHeaders {
		request.Header.Set(cacheControl, cacheControlNoCache)
	}

	queryParameters := []queryParamater{}
	if s.specVersion != nil {
		queryParameters = append(queryParameters, queryParamater{key: spec, value: common.StringFromRef(s.specVersion)})
	}
	queryParameters = append(queryParameters, queryParamater{key: since, value: strconv.FormatInt(s.changeNumber, 10)})
	if len(s.flagSetsFilter) > 0 {
		queryParameters = append(queryParameters, queryParamater{key: sets, value: s.flagSetsFilter})
	}
	if s.till != nil {
		queryParameters = append(queryParameters, queryParamater{key: till, value: strconv.FormatInt(*s.till, 10)})
	}

	request.URL.RawQuery = encode(queryParameters)
	return nil
}

type SegmentRequestParams struct {
	baseRequestParams
	changeNumber int64
	till         *int64
}

func MakeSegmentRequestParams() *SegmentRequestParams {
	return &SegmentRequestParams{
		baseRequestParams: baseRequestParams{
			cacheControlHeaders: true,
		},
	}
}

func (s *SegmentRequestParams) WithCacheControl(cacheControl bool) *SegmentRequestParams {
	s.cacheControlHeaders = cacheControl
	return s
}

func (s *SegmentRequestParams) WithChangeNumber(changeNumber int64) *SegmentRequestParams {
	s.changeNumber = changeNumber
	return s
}

func (s *SegmentRequestParams) WithTill(till int64) *SegmentRequestParams {
	s.till = common.Int64Ref(till)
	return s
}

func (s *SegmentRequestParams) ChangeNumber() int64 {
	return s.changeNumber
}

func (s *SegmentRequestParams) Apply(request *http.Request) error {
	if s.cacheControlHeaders {
		request.Header.Set(cacheControl, cacheControlNoCache)
	}

	queryParameters := []queryParamater{}
	queryParameters = append(queryParameters, queryParamater{key: since, value: strconv.FormatInt(s.changeNumber, 10)})
	if s.till != nil {
		queryParameters = append(queryParameters, queryParamater{key: till, value: strconv.FormatInt(*s.till, 10)})
	}

	request.URL.RawQuery = encode(queryParameters)
	return nil
}

type AuthRequestParams struct {
	baseRequestParams
	specVersion *string
}

func MakeAuthRequestParams(specVersion *string) *AuthRequestParams {
	return &AuthRequestParams{
		baseRequestParams: baseRequestParams{
			cacheControlHeaders: true,
		},
		specVersion: specVersion,
	}
}

func (s *AuthRequestParams) WithCacheControl(cacheControl bool) *AuthRequestParams {
	s.cacheControlHeaders = cacheControl
	return s
}

func (s *AuthRequestParams) Apply(request *http.Request) error {
	if s.cacheControlHeaders {
		request.Header.Set(cacheControl, cacheControlNoCache)
	}

	queryParams := request.URL.Query()
	if s.specVersion != nil {
		queryParams.Add(spec, common.StringFromRef(s.specVersion))
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
