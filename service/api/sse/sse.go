package sse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/splitio/go-toolkit/logging"
)

var sseDelimiter [2]byte = [...]byte{':', ' '}
var sseData [4]byte = [...]byte{'d', 'a', 't', 'a'}

// SSEClient struct
type SSEClient struct {
	url      string
	client   http.Client
	sseReady chan struct{}
	shutdown chan struct{}
	mainWG   sync.WaitGroup
	logger   logging.LoggerInterface
}

// NewSSEClient creates new SSEClient
func NewSSEClient(url string, ready chan struct{}, logger logging.LoggerInterface) *SSEClient {
	return &SSEClient{
		url:      url,
		client:   http.Client{},
		sseReady: ready,
		shutdown: make(chan struct{}, 1),
		mainWG:   sync.WaitGroup{},
		logger:   logger,
	}
}

// Shutdown stops SSE
func (l *SSEClient) Shutdown() {
	select {
	case l.shutdown <- struct{}{}:
	}
	l.mainWG.Wait()
}

// Event struct
type Event struct {
	Data string `json:"data"`
}

// NewEvent creates event
func NewEvent(raw []byte) (*Event, error) {
	e := Event{}
	err := json.Unmarshal(raw, &e)
	if err != nil {
		return nil, fmt.Errorf("error parsing 1st level json: %w", err)
	}
	return &e, nil
}

// Do starts streaming
func (l *SSEClient) Do(params map[string]string, callback func(e Event)) error {
	l.mainWG.Add(1)
	defer l.mainWG.Done()

	req, err := http.NewRequest("GET", l.url, nil)
	if err != nil {
		return errors.New("Could not create client")
	}

	query := req.URL.Query()

	for key, value := range params {
		query.Add(key, value)
	}
	req.URL.RawQuery = query.Encode()
	req.Header.Set("Accept", "text/event-stream")

	resp, err := l.client.Do(req)
	if err != nil {
		return errors.New("Could not perform request")
	}
	if resp.StatusCode != 200 {
		return errors.New("Could not connect to streaming")
	}

	l.sseReady <- struct{}{}
	reader := bufio.NewReader(resp.Body)
	defer resp.Body.Close()

	shouldKeepRunning := true
	activeGoroutines := sync.WaitGroup{}

	for shouldKeepRunning {
		select {
		case <-l.shutdown:
			l.logger.Info("Shutting down listener")
			shouldKeepRunning = false
			break
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				panic(err.Error())
			}

			if len(line) < 2 {
				continue
			}

			splitted := bytes.Split(line, sseDelimiter[:])
			if bytes.Compare(splitted[0], sseData[:]) != 0 {
				continue
			}

			data := bytes.TrimSpace(splitted[1])
			eventData, err := NewEvent(data)
			if err != nil {
				l.logger.Error("Error parsing event: ", err)
			}

			go func() {
				activeGoroutines.Add(1)
				callback(*eventData)
				activeGoroutines.Done()
			}()
		}
	}
	l.logger.Info("SSE streaming exiting")
	activeGoroutines.Wait()
	return nil
}
