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

type FetchOptions interface {
	Apply(request *http.Request) error
}

type BaseOptions struct {
	CacheControlHeaders bool
	SpecVersion         *string
}

type SplitFetchOptions struct {
	BaseOptions
	ChangeNumber   int64
	FlagSetsFilter string
	Till           *int64
}

func MakeSplitFetchOptions(specVersion *string) *SplitFetchOptions {
	return &SplitFetchOptions{
		BaseOptions: BaseOptions{
			CacheControlHeaders: true,
			SpecVersion:         specVersion,
		},
	}
}

func (s *SplitFetchOptions) WithChangeNumber(changeNumber int64) *SplitFetchOptions {
	s.ChangeNumber = changeNumber
	return s
}

func (s *SplitFetchOptions) WithFlagSetsFilter(flagSetsFilter string) *SplitFetchOptions {
	s.FlagSetsFilter = flagSetsFilter
	return s
}

func (s *SplitFetchOptions) WithTill(till int64) *SplitFetchOptions {
	s.Till = common.Int64Ref(till)
	return s
}

func (s *SplitFetchOptions) Apply(request *http.Request) error {
	request.Header.Set(cacheControl, cacheControlNoCache)

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

type SegmentFetchOptions struct {
	BaseOptions
	ChangeNumber int64
	Till         *int64
}

func MakeSegmentFetchOptions(specVersion *string) *SegmentFetchOptions {
	return &SegmentFetchOptions{
		BaseOptions: BaseOptions{
			CacheControlHeaders: true,
			SpecVersion:         specVersion,
		},
	}
}

func (s *SegmentFetchOptions) WithChangeNumber(changeNumber int64) *SegmentFetchOptions {
	s.ChangeNumber = changeNumber
	return s
}

func (s *SegmentFetchOptions) WithTill(till int64) *SegmentFetchOptions {
	s.Till = common.Int64Ref(till)
	return s
}

func (s *SegmentFetchOptions) Apply(request *http.Request) error {
	request.Header.Set(cacheControl, cacheControlNoCache)

	queryParameters := []queryParamater{}
	if s.SpecVersion != nil {
		queryParameters = append(queryParameters, queryParamater{key: spec, value: common.StringFromRef(s.SpecVersion)})
	}
	queryParameters = append(queryParameters, queryParamater{key: since, value: strconv.FormatInt(s.ChangeNumber, 10)})
	if s.Till != nil {
		queryParameters = append(queryParameters, queryParamater{key: till, value: strconv.FormatInt(*s.Till, 10)})
	}

	request.URL.RawQuery = encode(queryParameters)
	return nil
}

type AuthFetchOptions struct {
	BaseOptions
}

func MakeAuthFetchOptions(specVersion *string) *AuthFetchOptions {
	return &AuthFetchOptions{
		BaseOptions: BaseOptions{
			CacheControlHeaders: true,
			SpecVersion:         specVersion,
		},
	}
}

func (s *AuthFetchOptions) Apply(request *http.Request) error {
	request.Header.Set(cacheControl, cacheControlNoCache)

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
