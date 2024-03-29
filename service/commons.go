package service

import (
	"strconv"
)

const (
	cacheControl        = "Cache-Control"
	cacheControlNoCache = "no-cache"
)

type FetchOptions struct {
	CacheControlHeaders bool
	ChangeNumber        *int64
	FlagSetsFilter      string
}

func NewFetchOptions(cacheControlHeaders bool, changeNumber *int64) FetchOptions {
	return FetchOptions{
		CacheControlHeaders: cacheControlHeaders,
		ChangeNumber:        changeNumber,
	}
}

func (f *FetchOptions) SetFlagSetsFilter(flagSetsFilter string) {
	f.FlagSetsFilter = flagSetsFilter
}

func BuildFetch(changeNumber int64, fetchOptions *FetchOptions) (map[string]string, map[string]string) {
	queryParams := make(map[string]string)
	headers := make(map[string]string)
	queryParams["since"] = strconv.FormatInt(changeNumber, 10)

	if fetchOptions == nil {
		return queryParams, headers
	}
	if fetchOptions.CacheControlHeaders {
		headers[cacheControl] = cacheControlNoCache
	}
	if fetchOptions.ChangeNumber != nil {
		queryParams["till"] = strconv.FormatInt(*fetchOptions.ChangeNumber, 10)
	}
	if len(fetchOptions.FlagSetsFilter) > 0 {
		queryParams["sets"] = fetchOptions.FlagSetsFilter
	}
	return queryParams, headers
}
