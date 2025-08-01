package dtos

import (
	"fmt"
	"strings"
)

// Message type constants
const (
	MessageTypeUpdate = iota
	MessageTypeControl
	MessageTypeOccupancy
)

// Update type constants
const (
	UpdateTypeSplitChange        = "SPLIT_UPDATE"
	UpdateTypeSplitKill          = "SPLIT_KILL"
	UpdateTypeSegmentChange      = "SEGMENT_UPDATE"
	UpdateTypeContol             = "CONTROL"
	UpdateTypeLargeSegmentChange = "LS_DEFINITION_UPDATE"
	UpdateTypeRuleBasedChange    = "RB_SEGMENT_UPDATE"
)

// Control type constants
const (
	ControlTypeStreamingEnabled  = "STREAMING_ENABLED"
	ControlTypeStreamingPaused   = "STREAMING_PAUSED"
	ControlTypeStreamingDisabled = "STREAMING_DISABLED"
)

const (
	occupancyPrefix = "[?occupancy=metrics.publishers]"
)

// SSE event type constants
const (
	SSEEventTypeError   = "error"
	SSEEventTypeSync    = "sync"
	SSEEventTypeMessage = "message"
)

// Event basic interface
type Event interface {
	fmt.Stringer
	EventType() string
	Timestamp() int64
}

// SSESyncEvent represents an SSE Sync event with only id (used for resuming connections)
type SSESyncEvent struct {
	id        string
	timestamp int64
}

// EventType always returns SSEEventTypeSync for SSESyncEvents
func (e *SSESyncEvent) EventType() string { return SSEEventTypeSync }

// Timestamp returns the timestamp of the event parsing
func (e *SSESyncEvent) Timestamp() int64 { return e.timestamp }

// String returns the string represenation of the event
func (e *SSESyncEvent) String() string {
	return fmt.Sprintf("SSESync(id=%s,timestamp=%d)", e.id, e.timestamp)
}

// AblyError struct
type AblyError struct {
	code       int
	statusCode int
	message    string
	href       string
	timestamp  int64
}

func NewAblyError(code int, statusCode int, message string, href string, timestamp int64) *AblyError {
	return &AblyError{
		code:       code,
		statusCode: statusCode,
		message:    message,
		href:       href,
		timestamp:  timestamp,
	}
}

// EventType always returns SSEEventTypeError for AblyError
func (a *AblyError) EventType() string { return SSEEventTypeError }

// Code returns the error code
func (a *AblyError) Code() int { return a.code }

// StatusCode returns the status code
func (a *AblyError) StatusCode() int { return a.statusCode }

// Message returns the error message
func (a *AblyError) Message() string { return a.message }

// Href returns the documentation link
func (a *AblyError) Href() string { return a.href }

// Timestamp returns the error timestamp
func (a *AblyError) Timestamp() int64 { return a.timestamp }

// IsRetryable returns whether the error is recoverable via a push subsystem restart
func (a *AblyError) IsRetryable() bool { return a.code >= 40140 && a.code <= 40149 }

// String returns the string representation of the ably error
func (a *AblyError) String() string {
	return fmt.Sprintf("AblyError(code=%d,statusCode=%d,message=%s,timestamp=%d,isRetryable=%t)",
		a.code, a.statusCode, a.message, a.timestamp, a.IsRetryable())
}

// Message basic interface
type Message interface {
	Event
	MessageType() int64
	Channel() string
}

// BaseMessage contains the basic message-specific fields and methods
type BaseMessage struct {
	timestamp int64
	channel   string
}

func NewBaseMessage(timestamp int64, channel string) BaseMessage {
	return BaseMessage{
		timestamp: timestamp,
		channel:   channel,
	}
}

// EventType always returns SSEEventTypeMessage for BaseMessage and embedding types
func (m *BaseMessage) EventType() string { return SSEEventTypeMessage }

// Timestamp returns the timestamp of the message reception
func (m *BaseMessage) Timestamp() int64 { return m.timestamp }

// Channel returns which channel the message was received in
func (m *BaseMessage) Channel() string { return m.channel }

// OccupancyMessage contains fields & methods related to ocupancy messages
type OccupancyMessage struct {
	BaseMessage
	publishers int64
}

func NewOccupancyMessage(baseMessage BaseMessage, publishers int64) *OccupancyMessage {
	return &OccupancyMessage{
		BaseMessage: baseMessage,
		publishers:  publishers,
	}
}

// MessageType always returns MessageTypeOccupancy for Occupancy messages
func (o *OccupancyMessage) MessageType() int64 { return MessageTypeOccupancy }

// ChannelWithoutPrefix returns the original channel namem without the metadata prefix
func (o *OccupancyMessage) ChannelWithoutPrefix() string {
	return strings.Replace(o.Channel(), occupancyPrefix, "", 1)
}

// Publishers returbs the amount of publishers in the current channel
func (o *OccupancyMessage) Publishers() int64 {
	return o.publishers
}

// Strings returns the string representation of an occupancy message
func (o *OccupancyMessage) String() string {
	return fmt.Sprintf("Occupancy(channel=%s,publishers=%d,timestamp=%d)",
		o.Channel(), o.publishers, o.Timestamp())
}

// Update basic interface
type Update interface {
	Message
	UpdateType() string
	ChangeNumber() int64
}

// BaseUpdate contains fields & methods related to update-based messages
type BaseUpdate struct {
	BaseMessage
	changeNumber int64
}

func NewBaseUpdate(baseMessage BaseMessage, changeNumber int64) BaseUpdate {
	return BaseUpdate{
		BaseMessage:  baseMessage,
		changeNumber: changeNumber,
	}
}

// MessageType alwats returns MessageType for Update messages
func (b *BaseUpdate) MessageType() int64 { return MessageTypeUpdate }

// ChangeNumber returns the changeNumber of the update
func (b *BaseUpdate) ChangeNumber() int64 { return b.changeNumber }

// SplitChangeUpdate represents a SplitChange notification generated in the split servers
type SplitChangeUpdate struct {
	BaseUpdate
	previousChangeNumber *int64
	featureFlag          *SplitDTO
}

func NewSplitChangeUpdate(baseUpdate BaseUpdate, pcn *int64, featureFlag *SplitDTO) *SplitChangeUpdate {
	return &SplitChangeUpdate{
		BaseUpdate:           baseUpdate,
		previousChangeNumber: pcn,
		featureFlag:          featureFlag,
	}
}

// UpdateType always returns UpdateTypeSplitChange for SplitUpdate messages
func (u *SplitChangeUpdate) UpdateType() string { return UpdateTypeSplitChange }

// String returns the String representation of a split change notification
func (u *SplitChangeUpdate) String() string {
	return fmt.Sprintf("SplitChange(channel=%s,changeNumber=%d,timestamp=%d)",
		u.Channel(), u.ChangeNumber(), u.Timestamp())
}

// PreviousChangeNumber returns previous change number
func (u *SplitChangeUpdate) PreviousChangeNumber() *int64 { return u.previousChangeNumber }

// FeatureFlag returns feature flag definiiton or nil
func (u *SplitChangeUpdate) FeatureFlag() *SplitDTO { return u.featureFlag }

// SplitKillUpdate represents a SplitKill notification generated in the split servers
type SplitKillUpdate struct {
	BaseUpdate
	splitName        string
	defaultTreatment string
}

func NewSplitKillUpdate(baseUpdate BaseUpdate, splitName string, defaultTreatment string) *SplitKillUpdate {
	return &SplitKillUpdate{
		BaseUpdate:       baseUpdate,
		splitName:        splitName,
		defaultTreatment: defaultTreatment,
	}
}

// UpdateType always returns UpdateTypeSplitKill for SplitKillUpdate messages
func (u *SplitKillUpdate) UpdateType() string { return UpdateTypeSplitKill }

// SplitName returns the name of the killed split
func (u *SplitKillUpdate) SplitName() string { return u.splitName }

// DefaultTreatment returns the last default treatment seen in the split servers for this split
func (u *SplitKillUpdate) DefaultTreatment() string { return u.defaultTreatment }

// ToSplitChangeUpdate Maps this kill notification to a split change one
func (u *SplitKillUpdate) ToSplitChangeUpdate() *SplitChangeUpdate {
	return &SplitChangeUpdate{BaseUpdate: u.BaseUpdate}
}

// String returns the string representation of this update
func (u *SplitKillUpdate) String() string {
	return fmt.Sprintf("SplitKill(channel=%s,changeNumber=%d,splitName=%s,defaultTreatment=%s,timestamp=%d)",
		u.Channel(), u.ChangeNumber(), u.SplitName(), u.DefaultTreatment(), u.Timestamp())
}

// SegmentChangeUpdate represents a segment change notification generated in the split servers.
type SegmentChangeUpdate struct {
	BaseUpdate
	segmentName string
}

func NewSegmentChangeUpdate(baseUpdate BaseUpdate, segmentName string) *SegmentChangeUpdate {
	return &SegmentChangeUpdate{
		BaseUpdate:  baseUpdate,
		segmentName: segmentName,
	}
}

// UpdateType is always UpdateTypeSegmentChange for Segmet Updates
func (u *SegmentChangeUpdate) UpdateType() string { return UpdateTypeSegmentChange }

// SegmentName returns the name of the updated segment
func (u *SegmentChangeUpdate) SegmentName() string { return u.segmentName }

// String returns the string representation of a segment update notification
func (u *SegmentChangeUpdate) String() string {
	return fmt.Sprintf("SegmentChange(channel=%s,changeNumber=%d,segmentName=%s,timestamp=%d)",
		u.Channel(), u.ChangeNumber(), u.segmentName, u.Timestamp())
}

// ControlUpdate represents a control notification generated by the split push subsystem
type ControlUpdate struct {
	BaseMessage
	controlType string
}

func NewControlUpdate(baseMessage BaseMessage, controlType string) *ControlUpdate {
	return &ControlUpdate{
		BaseMessage: baseMessage,
		controlType: controlType,
	}
}

// MessageType always returns MessageTypeControl for Control messages
func (u *ControlUpdate) MessageType() int64 { return MessageTypeControl }

// ControlType returns the type of control notification received
func (u *ControlUpdate) ControlType() string { return u.controlType }

// String returns a string representation of this notification
func (u *ControlUpdate) String() string {
	return fmt.Sprintf("Control(channel=%s,type=%s,timestamp=%d)",
		u.Channel(), u.controlType, u.Timestamp())
}

type LargeSegmentChangeUpdate struct {
	BaseUpdate
	LargeSegments []LargeSegmentRFDResponseDTO `json:"ls"`
}

func NewLargeSegmentChangeUpdate(baseUpdate BaseUpdate, largeSegments []LargeSegmentRFDResponseDTO) *LargeSegmentChangeUpdate {
	return &LargeSegmentChangeUpdate{
		BaseUpdate:    baseUpdate,
		LargeSegments: largeSegments,
	}
}

// UpdateType is always UpdateTypeLargeSegmentChange for Large Segmet Updates
func (u *LargeSegmentChangeUpdate) UpdateType() string { return UpdateTypeLargeSegmentChange }

// String returns the string representation of a segment update notification
func (u *LargeSegmentChangeUpdate) String() string {
	return fmt.Sprintf("LargeSegmentChange(channel=%s,changeNumber=%d,count=%d,timestamp=%d)",
		u.Channel(), u.ChangeNumber(), len(u.LargeSegments), u.Timestamp())
}

// SplitChangeUpdate represents a SplitChange notification generated in the split servers
type RuleBasedChangeUpdate struct {
	BaseUpdate
	previousChangeNumber *int64
	ruleBasedSegment     *RuleBasedSegmentDTO
}

func NewRuleBasedChangeUpdate(baseUpdate BaseUpdate, pcn *int64, ruleBasedSegment *RuleBasedSegmentDTO) *RuleBasedChangeUpdate {
	return &RuleBasedChangeUpdate{
		BaseUpdate:           baseUpdate,
		previousChangeNumber: pcn,
		ruleBasedSegment:     ruleBasedSegment,
	}
}

// UpdateType is always UpdateTypeRuleBasedSegmentChange for Rule-based Segmet Updates
func (u *RuleBasedChangeUpdate) UpdateType() string { return UpdateTypeRuleBasedChange }

// String returns the string representation of a segment update notification
func (u *RuleBasedChangeUpdate) String() string {
	return fmt.Sprintf("LargeSegmentChange(channel=%s,changeNumber=%d,timestamp=%d)",
		u.Channel(), u.ChangeNumber(), u.Timestamp())
}

// PreviousChangeNumber returns previous change number
func (u *RuleBasedChangeUpdate) PreviousChangeNumber() *int64 { return u.previousChangeNumber }

// RuleBasedSegment returns rule-based segment definiiton or nil
func (u *RuleBasedChangeUpdate) RuleBasedsegment() *RuleBasedSegmentDTO { return u.ruleBasedSegment }

// Compile-type assertions of interface requirements
var _ Event = &AblyError{}
var _ Message = &OccupancyMessage{}
var _ Message = &SplitChangeUpdate{}
var _ Message = &SplitKillUpdate{}
var _ Message = &SegmentChangeUpdate{}
var _ Message = &ControlUpdate{}
var _ Message = &LargeSegmentChangeUpdate{}
var _ Message = &RuleBasedChangeUpdate{}
var _ Update = &SplitChangeUpdate{}
var _ Update = &SplitKillUpdate{}
var _ Update = &SegmentChangeUpdate{}
var _ Update = &LargeSegmentChangeUpdate{}
var _ Update = &RuleBasedChangeUpdate{}
