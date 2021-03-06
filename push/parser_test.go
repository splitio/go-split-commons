package push

import (
	"encoding/json"
	"testing"

	"github.com/splitio/go-toolkit/common"
	"github.com/splitio/go-toolkit/logging"
	sseMocks "github.com/splitio/go-toolkit/sse/mocks"
)

func TestParseSplitUpdate(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:         UpdateTypeSplitChange,
				ChangeNumber: 123,
			})
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      string(updateJSON),
				Channel:   "sarasa_splits",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onSplitUpdate: func(u *SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error("change number should be 123. Is: ", u.changeNumber)
			}
			if u.Channel() != "sarasa_splits" {
				t.Error("channel should be sarasa_splits. Is:", u.channel)
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestParseSplitKill(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:             UpdateTypeSplitKill,
				ChangeNumber:     123,
				SplitName:        "someSplit",
				DefaultTreatment: "off",
			})
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      string(updateJSON),
				Channel:   "sarasa_splits",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onSplitKill: func(u *SplitKillUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error("change number should be 123. Is: ", u.changeNumber)
			}
			if u.Channel() != "sarasa_splits" {
				t.Error("channel should be sarasa_splits. Is:", u.channel)
			}
			if u.SplitName() != "someSplit" {
				t.Error("split name should be someSplit. Is: ", u.SplitName())
			}
			if u.DefaultTreatment() != "off" {
				t.Error("default treatment should be off. Is: ", u.DefaultTreatment())
			}

			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestParseSegmentChange(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:         UpdateTypeSegmentChange,
				ChangeNumber: 123,
				SegmentName:  "someSegment",
			})
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      string(updateJSON),
				Channel:   "sarasa_segments",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onSegmentUpdate: func(u *SegmentChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error("change number should be 123. Is: ", u.changeNumber)
			}
			if u.Channel() != "sarasa_segments" {
				t.Error("channel should be sarasa_splits. Is:", u.channel)
			}
			if u.SegmentName() != "someSegment" {
				t.Error("segment name should be someSegment. Is: ", u.SegmentName())
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestControl(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:        UpdateTypeContol,
				ControlType: ControlTypeStreamingDisabled,
			})
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      string(updateJSON),
				Channel:   "control_pri",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onControlUpdate: func(u *ControlUpdate) *int64 {
			if u.Channel() != "control_pri" {
				t.Error("channel should be sarasa_splits. Is:", u.Channel())
			}
			if u.ControlType() != ControlTypeStreamingDisabled {
				t.Error("incorrect control type. Expected: ", u.ControlType())
			}
			return common.Int64Ref(123)
		},
	}

	if status, err := parser.ParseAndForward(event); *status != 123 || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestOccupancy(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:    SSEEventTypeMessage,
				Metrics: metrics{Publishers: 12},
			})
			mainJSON, _ := json.Marshal(genericData{
				Name:      occupancuName,
				Timestamp: 123,
				Data:      string(updateJSON),
				Channel:   "control_pri",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onOccupancyMesage: func(u *OccupancyMessage) *int64 {
			if u.Channel() != "control_pri" {
				t.Error("channel should be sarasa_splits. Is:", u.Channel())
			}
			if u.Publishers() != 12 {
				t.Error("there should be 12 publishers. Got: ", u.Publishers())
			}
			return common.Int64Ref(123)
		},
	}

	if status, err := parser.ParseAndForward(event); *status != 123 || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestAblyError(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeError },
		DataCall: func() string {
			mainJSON, _ := json.Marshal(genericData{
				Timestamp:  123,
				Code:       1,
				StatusCode: 2,
				Message:    "abc",
				Href:       "def",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onAblyError: func(u *AblyError) *int64 {
			if u.Timestamp() != 123 {
				t.Error("invalid timestamp")
			}
			if u.Code() != 1 {
				t.Error("invalid code")
			}
			if u.StatusCode() != 2 {
				t.Error("invalid status code")
			}
			if u.Message() != "abc" {
				t.Error("invalid message")
			}
			if u.Href() != "def" {
				t.Error("invalid href")
			}
			return common.Int64Ref(123)
		},
	}

	if status, err := parser.ParseAndForward(event); *status != 123 || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestSyncMessage(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:      func() string { return "abc" },
		EventCall:   func() string { return "" },
		DataCall:    func() string { return "" },
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{logger: logger}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error("no error should have been returned. Got: ", err)
	}
}

func TestEmptyError(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:      func() string { return "" },
		EventCall:   func() string { return "" },
		DataCall:    func() string { return "" },
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{logger: logger}

	if status, err := parser.ParseAndForward(event); status != nil || err != ErrEmptyEvent {
		t.Error("invalid status or returned error", err)
	}
}

func TestOuterDataJSONError(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:      func() string { return "abc" },
		EventCall:   func() string { return SSEEventTypeMessage },
		DataCall:    func() string { return "{" },
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{logger: logger}

	if status, err := parser.ParseAndForward(event); status != nil || err == nil {
		t.Error("invalid status or returned error", err)
	}
}

func TestInnerDataJSONError(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return SSEEventTypeMessage },
		DataCall: func() string {
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      "{",
				Channel:   "sarasa_splits",
			})
			return string(mainJSON)
		},
		IsErrorCall: func() bool { return false },
		IsEmptyCall: func() bool { return false },
		RetryCall:   func() int64 { return 0 },
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{logger: logger}

	if status, err := parser.ParseAndForward(event); status != nil || err == nil {
		t.Error("invalid status or returned error", err)
	}
}
