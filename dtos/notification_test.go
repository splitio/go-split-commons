package dtos

import (
	"testing"

	"github.com/splitio/go-toolkit/v5/common"
)

func TestSSESyncEvent(t *testing.T) {
	sseSyncEvent := SSESyncEvent{id: "1", timestamp: 123456789}
	if sseSyncEvent.EventType() != SSEEventTypeSync {
		t.Error("Unexpected EventType")
	}
	if sseSyncEvent.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if sseSyncEvent.String() != "SSESync(id=1,timestamp=123456789)" {
		t.Error("Unexpected String")
	}
}

func TestAblyError(t *testing.T) {
	ablyError := NewAblyError(40145, 1, "error", "href", 123456789)
	if ablyError.EventType() != SSEEventTypeError {
		t.Error("Unexpected EventType")
	}
	if ablyError.Code() != 40145 {
		t.Error("Unexpected Code")
	}
	if ablyError.StatusCode() != 1 {
		t.Error("Unexpected StatusCode")
	}
	if ablyError.Message() != "error" {
		t.Error("Unexpected StatusCode")
	}
	if ablyError.Href() != "href" {
		t.Error("Unexpected Href")
	}
	if ablyError.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if !ablyError.IsRetryable() {
		t.Error("It should be RetryableError")
	}
	if ablyError.String() != "AblyError(code=40145,statusCode=1,message=error,timestamp=123456789,isRetryable=true)" {
		t.Error("Unexpected string")
	}
}

func TestBaseMessage(t *testing.T) {
	baseMessage := NewBaseMessage(123456789, "some_channel")
	if baseMessage.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if baseMessage.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if baseMessage.Channel() != "some_channel" {
		t.Error("Unexpected Channel")
	}
}

func TestOccupancyMessage(t *testing.T) {
	occupancyMessage := NewOccupancyMessage(NewBaseMessage(123456789, "[?occupancy=metrics.publishers]some_channel"), 2)
	if occupancyMessage.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if occupancyMessage.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if occupancyMessage.Channel() != "[?occupancy=metrics.publishers]some_channel" {
		t.Error("Unexpected Channel")
	}
	if occupancyMessage.MessageType() != MessageTypeOccupancy {
		t.Error("Unexpected MessageType")
	}
	if occupancyMessage.ChannelWithoutPrefix() != "some_channel" {
		t.Error("Unexpected Channel")
	}
	if occupancyMessage.Publishers() != 2 {
		t.Error("Unexpected Publishers")
	}
	if occupancyMessage.String() != "Occupancy(channel=[?occupancy=metrics.publishers]some_channel,publishers=2,timestamp=123456789)" {
		t.Error("Unexpected string")
	}
}

func TestBaseUpdate(t *testing.T) {
	baseUpdate := NewBaseUpdate(NewBaseMessage(123456789, "some_channel"), 123456)
	if baseUpdate.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if baseUpdate.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if baseUpdate.Channel() != "some_channel" {
		t.Error("Unexpected Channel")
	}
	if baseUpdate.MessageType() != MessageTypeUpdate {
		t.Error("Unexpected MessageType")
	}
	if baseUpdate.ChangeNumber() != 123456 {
		t.Error("Unexpected ChangeNumber")
	}
}

func TestSplitChangeUpdate(t *testing.T) {
	splitUpdate := NewSplitChangeUpdate(
		NewBaseUpdate(NewBaseMessage(123456789, "some_channel"), 123456),
		common.Int64Ref(123),
		&SplitDTO{Name: "some_split"})
	if splitUpdate.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if splitUpdate.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if splitUpdate.Channel() != "some_channel" {
		t.Error("Unexpected Channel")
	}
	if splitUpdate.MessageType() != MessageTypeUpdate {
		t.Error("Unexpected MessageType")
	}
	if splitUpdate.ChangeNumber() != 123456 {
		t.Error("Unexpected ChangeNumber")
	}
	if splitUpdate.UpdateType() != UpdateTypeSplitChange {
		t.Error("Unexpected UpdateType")
	}
	if splitUpdate.String() != "SplitChange(channel=some_channel,changeNumber=123456,timestamp=123456789)" {
		t.Error("Unexpected String")
	}
	if *splitUpdate.PreviousChangeNumber() != 123 {
		t.Error("Unexpected PreviousChangeNumber")
	}
	if splitUpdate.FeatureFlag().Name != "some_split" {
		t.Error("Unexpected FeatureFlag")
	}
}

func TestSplitKillUpdate(t *testing.T) {
	splitKillUpdate := NewSplitKillUpdate(
		NewBaseUpdate(NewBaseMessage(123456789, "some_channel"), 123456),
		"some_split", "default_treatment")
	if splitKillUpdate.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if splitKillUpdate.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if splitKillUpdate.Channel() != "some_channel" {
		t.Error("Unexpected Channel")
	}
	if splitKillUpdate.MessageType() != MessageTypeUpdate {
		t.Error("Unexpected MessageType")
	}
	if splitKillUpdate.ChangeNumber() != 123456 {
		t.Error("Unexpected ChangeNumber")
	}
	if splitKillUpdate.UpdateType() != UpdateTypeSplitKill {
		t.Error("Unexpected UpdateType")
	}
	if splitKillUpdate.SplitName() != "some_split" {
		t.Error("Unexpected SplitName")
	}
	if splitKillUpdate.DefaultTreatment() != "default_treatment" {
		t.Error("Unexpected DefaultTreatment")
	}
	if splitKillUpdate.ToSplitChangeUpdate().UpdateType() != UpdateTypeSplitChange {
		t.Error("Unexpected ToSplitChangeUpdate")
	}
	if splitKillUpdate.String() != "SplitKill(channel=some_channel,changeNumber=123456,splitName=some_split,defaultTreatment=default_treatment,timestamp=123456789)" {
		t.Error("Unexpected String")
	}
}

func TestSegmentChangeUpdate(t *testing.T) {
	segmentUpdate := NewSegmentChangeUpdate(
		NewBaseUpdate(NewBaseMessage(123456789, "some_channel"), 123456),
		"some_segment")
	if segmentUpdate.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if segmentUpdate.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if segmentUpdate.Channel() != "some_channel" {
		t.Error("Unexpected Channel")
	}
	if segmentUpdate.MessageType() != MessageTypeUpdate {
		t.Error("Unexpected MessageType")
	}
	if segmentUpdate.ChangeNumber() != 123456 {
		t.Error("Unexpected ChangeNumber")
	}
	if segmentUpdate.UpdateType() != UpdateTypeSegmentChange {
		t.Error("Unexpected UpdateType")
	}
	if segmentUpdate.SegmentName() != "some_segment" {
		t.Error("Unexpected SegmentName")
	}
	if segmentUpdate.String() != "SegmentChange(channel=some_channel,changeNumber=123456,segmentName=some_segment,timestamp=123456789)" {
		t.Error("Unexpected String", segmentUpdate.String())
	}
}

func TestControlUpdate(t *testing.T) {
	controlUpdate := NewControlUpdate(NewBaseMessage(123456789, "some_channel"), ControlTypeStreamingPaused)
	if controlUpdate.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if controlUpdate.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if controlUpdate.Channel() != "some_channel" {
		t.Error("Unexpected Channel")
	}
	if controlUpdate.MessageType() != MessageTypeControl {
		t.Error("Unexpected MessageType")
	}
	if controlUpdate.ControlType() != ControlTypeStreamingPaused {
		t.Error("Unexpected ControlType")
	}
	if controlUpdate.String() != "Control(channel=some_channel,type=STREAMING_PAUSED,timestamp=123456789)" {
		t.Error("Unexpected String")
	}
}

func TestLargeSegmentChangeUpdate(t *testing.T) {
	ls := []LargeSegmentRFDResponseDTO{
		{
			Name:             "ls1",
			NotificationType: UpdateTypeLargeSegmentChange,
			SpecVersion:      "1.0",
			RFD:              &RFD{},
			ChangeNumber:     123123,
		},
	}

	lsUpdate := NewLargeSegmentChangeUpdate(NewBaseUpdate(NewBaseMessage(123456789, "ls_channel"), 123456), ls)
	if lsUpdate.EventType() != SSEEventTypeMessage {
		t.Error("Unexpected EventType")
	}
	if lsUpdate.Timestamp() != 123456789 {
		t.Error("Unexpected Timestamp")
	}
	if lsUpdate.Channel() != "ls_channel" {
		t.Error("Unexpected Channel")
	}
	if lsUpdate.MessageType() != MessageTypeUpdate {
		t.Error("Unexpected MessageType")
	}
	if lsUpdate.ChangeNumber() != 123456 {
		t.Error("Unexpected ChangeNumber")
	}
	if lsUpdate.UpdateType() != UpdateTypeLargeSegmentChange {
		t.Error("Unexpected UpdateType")
	}
	if lsUpdate.String() != "LargeSegmentChange(channel=ls_channel,changeNumber=123456,count=1,timestamp=123456789)" {
		t.Error("Unexpected String", lsUpdate.String())
	}
	if len(lsUpdate.LargeSegments) != 1 {
		t.Error("LargeSegments len should be 1. Actual: ", len(lsUpdate.LargeSegments))
	}
}
