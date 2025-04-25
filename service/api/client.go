package api

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/splitio/go-split-commons/v7/conf"
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Cache control header constants
const (
	CacheControlHeader  = "Cache-Control"
	CacheControlNoCache = "no-cache"
)

// Client interface for HTTPClient
type Client interface {
	Get(endpoint string, fetchOptions service.RequestParams) ([]byte, error)
	Post(endpoint string, body []byte, headers map[string]string) error
}

// HTTPClient structure to wrap up the net/http.Client
type HTTPClient struct {
	url        string
	httpClient *http.Client
	logger     logging.LoggerInterface
	apikey     string
	metadata   dtos.Metadata
}

// NewHTTPClient instance of HttpClient
func NewHTTPClient(
	apikey string,
	cfg conf.AdvancedConfig,
	endpoint string,
	logger logging.LoggerInterface,
	metadata dtos.Metadata,
) Client {
	timeout := cfg.HTTPTimeout
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}
	return &HTTPClient{
		url:        endpoint,
		httpClient: client,
		logger:     logger,
		apikey:     apikey,
		metadata:   metadata,
	}
}

// Get method is a get call to an url
func (c *HTTPClient) Get(endpoint string, fetchOptions service.RequestParams) ([]byte, error) {
	serviceURL := c.url + endpoint
	req, _ := http.NewRequest("GET", serviceURL, nil)
	authorization := c.apikey
	c.logger.Debug("Authorization [ApiKey]: ", logging.ObfuscateAPIKey(authorization))
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Content-Type", "application/json")
	parsedHeaders := AddMetadataToHeaders(c.metadata, make(map[string]string), nil)

	for headerName, headerValue := range parsedHeaders {
		req.Header.Add(headerName, headerValue)
	}

	c.logger.Debug(fmt.Sprintf("Headers: %v", req.Header))

	req.Header.Add("Authorization", "Bearer "+authorization)

	if fetchOptions != nil {
		fetchOptions.Apply(req)
	}

	c.logger.Debug("[GET] ", req.URL.String())
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error("Error requesting data to API: ", req.URL.String(), err.Error())
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := fmt.Sprintf("GET method: [%s] Status Code: %d - %s", req.URL.String(), resp.StatusCode, resp.Status)
		if resp.StatusCode == http.StatusNotModified {
			c.logger.Debug(message)
		} else {
			c.logger.Error(message)
		}

		return nil, &dtos.HTTPError{
			Code:    resp.StatusCode,
			Message: resp.Status,
		}
	}
	defer resp.Body.Close()

	// Check that the server actually sent compressed data
	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error parsing gzip resopnse body: %w", err)
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		c.logger.Error("error readig body from: ", req.URL.String(), ": ", err.Error())
		return nil, err
	}

	c.logger.Verbose("[RESPONSE_BODY]", string(body), "[END_RESPONSE_BODY]")
	return body, nil
}

// Post performs a HTTP POST request
func (c *HTTPClient) Post(endpoint string, body []byte, headers map[string]string) error {

	serviceURL := c.url + endpoint
	c.logger.Debug("[POST] ", serviceURL)
	req, _ := http.NewRequest("POST", serviceURL, bytes.NewBuffer(body))
	//****************
	req.Close = true // To prevent EOF error when connection is closed
	//****************
	authorization := c.apikey
	c.logger.Debug("Authorization [ApiKey]: ", logging.ObfuscateAPIKey(authorization))

	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Content-Type", "application/json")

	for headerName, headerValue := range headers {
		req.Header.Add(headerName, headerValue)
	}

	c.logger.Debug(fmt.Sprintf("Headers: %v", req.Header))

	req.Header.Add("Authorization", "Bearer "+authorization)

	c.logger.Verbose("[REQUEST_BODY]", string(body), "[END_REQUEST_BODY]")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error("Error posting data to API: ", req.URL.String(), err.Error())
		return err
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.logger.Error(err.Error())
		return err
	}

	c.logger.Verbose("[RESPONSE_BODY]", string(respBody), "[END_RESPONSE_BODY]")

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	c.logger.Error(fmt.Sprintf("POST [%s] Status Code: %d - %s", req.URL.String(), resp.StatusCode, resp.Status))
	return &dtos.HTTPError{
		Code:    resp.StatusCode,
		Message: resp.Status,
	}
}
