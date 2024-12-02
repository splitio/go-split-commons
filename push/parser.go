package push

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service/api/sse"

	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/datautils"
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	occupancyName = "[meta]occupancy"
)

func getCompressType(number *int) *int {
	if number == nil || *number > 2 {
		return nil
	}
	return number
}

// ErrEmptyEvent indicates an event without message and event fields
var ErrEmptyEvent = errors.New("empty incoming event")

// NotificationParser interface
type NotificationParser interface {
	ParseAndForward(sse.IncomingMessage) (*int64, error)
}

// NotificationParserImpl implementas the NotificationParser interface
type NotificationParserImpl struct {
	dataUtils            DataUtils
	logger               logging.LoggerInterface
	onSplitUpdate        func(*dtos.SplitChangeUpdate) error
	onSplitKill          func(*dtos.SplitKillUpdate) error
	onSegmentUpdate      func(*dtos.SegmentChangeUpdate) error
	onLargeSegmentUpdate func(*dtos.LargeSegmentChangeUpdate) error
	onControlUpdate      func(*dtos.ControlUpdate) *int64
	onOccupancyMesage    func(*dtos.OccupancyMessage) *int64
	onAblyError          func(*dtos.AblyError) *int64
}

func NewNotificationParserImpl(
	loggerInterface logging.LoggerInterface,
	onSplitUpdate func(update *dtos.SplitChangeUpdate) error,
	onSplitKill func(*dtos.SplitKillUpdate) error,
	onSegmentUpdate func(*dtos.SegmentChangeUpdate) error,
	onControlUpdate func(*dtos.ControlUpdate) *int64,
	onOccupancyMessage func(*dtos.OccupancyMessage) *int64,
	onAblyError func(*dtos.AblyError) *int64,
	onLargeSegmentUpdate func(*dtos.LargeSegmentChangeUpdate) error) *NotificationParserImpl {
	return &NotificationParserImpl{
		dataUtils:            NewDataUtilsImpl(),
		logger:               loggerInterface,
		onSplitUpdate:        onSplitUpdate,
		onSplitKill:          onSplitKill,
		onSegmentUpdate:      onSegmentUpdate,
		onControlUpdate:      onControlUpdate,
		onOccupancyMesage:    onOccupancyMessage,
		onAblyError:          onAblyError,
		onLargeSegmentUpdate: onLargeSegmentUpdate,
	}
}

// ParseAndForward accepts an incoming RAW event and returns a properly parsed & typed event
func (p *NotificationParserImpl) ParseAndForward(raw sse.IncomingMessage) (*int64, error) {

	if raw.Event() == "" {
		if raw.ID() == "" {
			return nil, ErrEmptyEvent
		}
		// If it has ID its a sync event, which we're not using not. Ignore.
		p.logger.Debug("Ignoring sync event")
		return nil, nil
	}

	data := genericData{}
	err := json.Unmarshal([]byte(raw.Data()), &data)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON: %w", err)
	}

	switch raw.Event() {
	case dtos.SSEEventTypeError:
		return p.parseError(&data)
	case dtos.SSEEventTypeMessage:
		return p.parseMessage(&data)
	}

	return nil, nil

}

func (p *NotificationParserImpl) parseError(data *genericData) (*int64, error) {
	return p.onAblyError(dtos.NewAblyError(data.Code, data.StatusCode, data.Message, data.Href, data.Timestamp)), nil
}

func (p *NotificationParserImpl) parseMessage(data *genericData) (*int64, error) {
	var nested genericMessageData
	err := json.Unmarshal([]byte(data.Data), &nested)
	if err != nil {
		return nil, fmt.Errorf("error parsing message nested json data: %w", err)
	}

	if data.Name == occupancyName {
		return p.onOccupancyMesage(dtos.NewOccupancyMessage(
			dtos.NewBaseMessage(data.Timestamp, data.Channel),
			nested.Metrics.Publishers),
		), nil
	}

	return p.parseUpdate(data, &nested)
}

func (p *NotificationParserImpl) parseUpdate(data *genericData, nested *genericMessageData) (*int64, error) {
	if data == nil || nested == nil {
		return nil, errors.New("parseUpdate: data cannot be nil")
	}

	base := dtos.NewBaseUpdate(dtos.NewBaseMessage(data.Timestamp, data.Channel), nested.ChangeNumber)

	switch nested.Type {
	case dtos.UpdateTypeSplitChange:
		featureFlag := p.processMessage(nested)
		if featureFlag == nil {
			return nil, p.onSplitUpdate(dtos.NewSplitChangeUpdate(base, nil, nil))
		}
		return nil, p.onSplitUpdate(dtos.NewSplitChangeUpdate(base, &nested.PreviousChangeNumber, featureFlag))
	case dtos.UpdateTypeSplitKill:
		return nil, p.onSplitKill(dtos.NewSplitKillUpdate(base, nested.SplitName, nested.DefaultTreatment))
	case dtos.UpdateTypeSegmentChange:
		return nil, p.onSegmentUpdate(dtos.NewSegmentChangeUpdate(base, nested.SegmentName))
	case dtos.UpdateTypeLargeSegmentChange:
		largeSegments := p.processLargeSegmentMessage(nested)
		return nil, p.onLargeSegmentUpdate(dtos.NewLargeSegmentChangeUpdate(base, largeSegments))
	case dtos.UpdateTypeContol:
		return p.onControlUpdate(dtos.NewControlUpdate(base.BaseMessage, nested.ControlType)), nil
	default:
		// TODO: log full event in debug mode
		return nil, fmt.Errorf("invalid update type: %s", nested.Type)
	}
}

func (p *NotificationParserImpl) processLargeSegmentMessage(nested *genericMessageData) []dtos.LargeSegmentRFDResponseDTO {
	if nested.LargeSegments == nil {
		p.logger.Debug("error reading nested message, LargeSegments property is nil")
		return []dtos.LargeSegmentRFDResponseDTO{}
	}

	return nested.LargeSegments
}

func (p *NotificationParserImpl) processMessage(nested *genericMessageData) *dtos.SplitDTO {
	compressType := getCompressType(nested.CompressType)
	if nested.FeatureFlagDefinition == nil || compressType == nil {
		return nil
	}
	ffDecoded, err := p.dataUtils.Decode(common.StringFromRef(nested.FeatureFlagDefinition))
	if err != nil {
		p.logger.Debug(fmt.Sprintf("error decoding FeatureFlagDefinition: '%s'", err.Error()))
		return nil
	}
	if common.IntFromRef(compressType) != datautils.None {
		ffDecoded, err = p.dataUtils.Decompress(ffDecoded, common.IntFromRef(compressType))
		if err != nil {
			p.logger.Debug(fmt.Sprintf("error decompressing FeatureFlagDefinition: '%s'", err.Error()))
			return nil
		}
	}

	var featureFlag dtos.SplitDTO
	err = json.Unmarshal([]byte(ffDecoded), &featureFlag)
	if err != nil {
		p.logger.Debug(fmt.Sprintf("error parsing feature flag json definition: '%s'", err.Error()))
		return nil
	}
	return &featureFlag
}

type genericData struct {

	// Error associated data
	Code       int    `json:"code"`
	StatusCode int    `json:"statusCode"`
	Message    string `json:"message"`
	Href       string `json:"href"`

	ClientID  string `json:"clientId"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
	Encoding  string `json:"encoding"`
	Channel   string `json:"channel"`
	Data      string `json:"data"`

	//"id":"tO4rXGE4CX:0:0","timestamp":1612897630627,"encoding":"json","channel":"[?occupancy=metrics.publishers]control_sec","data":"{\"metrics\":{\"publishers\":0}}","name":"[meta]occupancy"}

}

type metrics struct {
	Publishers int64 `json:"publishers"`
}

type genericMessageData struct {
	Metrics               metrics                           `json:"metrics"`
	Type                  string                            `json:"type"`
	ChangeNumber          int64                             `json:"changeNumber"`
	SplitName             string                            `json:"splitName"`
	DefaultTreatment      string                            `json:"defaultTreatment"`
	SegmentName           string                            `json:"segmentName"`
	ControlType           string                            `json:"controlType"`
	PreviousChangeNumber  int64                             `json:"pcn"`
	CompressType          *int                              `json:"c"`
	FeatureFlagDefinition *string                           `json:"d"`
	LargeSegments         []dtos.LargeSegmentRFDResponseDTO `json:"ls"`

	// {\"type\":\"SPLIT_UPDATE\",\"changeNumber\":1612909342671}"}
}
