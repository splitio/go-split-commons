package push

import (
	"encoding/json"
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
	sseMocks "github.com/splitio/go-toolkit/v5/sse/mocks"
)

const FF_SHOULD_BE_NIL = "feature flag dto should be nil"
const FF_NOT_SHOULD_BE_NIL = "feature flag dto should be not nil"
const CN_SHOULD_BE_123 = "change number should be 123. Is: "
const CHANNEL_SHOULD_BE = "channel should be sarasa_splits. Is: "
const ERROR_SHOULD_RETURNED = "no error should have been returned. Got: "
const FF_SHOULD_BE_MAURO_JAVA = "feature flag should be mauro_java"
const FF_DEFINITION_ZLIB = "eJzMk99u2kwQxV8lOtdryQZj8N6hD5QPlThSTVNVEUKDPYZt1jZar1OlyO9emf8lVFWv2ss5zJyd82O8hTWUZSqZvW04opwhUVdsIKBSSKR+10vS1HWW7pIdz2NyBjRwHS8IXEopTLgbQqDYT+ZUm3LxlV4J4mg81LpMyKqygPRc94YeM6eQTtjphp4fegLVXvD6Qdjt9wPXF6gs2bqCxPC/2eRpDIEXpXXblpGuWCDljGptZ4bJ5lxYSJRZBoFkTcWKozpfsoH0goHfCXpB6PfcngDpVQnZEUjKIlOr2uwWqiC3zU5L1aF+3p7LFhUkPv8/mY2nk3gGgZxssmZzb8p6A9n25ktVtA9iGI3ODXunQ3HDp+AVWT6F+rZWlrWq7MN+YkSWWvuTDvkMSnNV7J6oTdl6qKTEvGnmjcCGjL2IYC/ovPYgUKnvvPtbmrmApiVryLM7p2jE++AfH6fTx09/HvuF32LWnNjStM0Xh3c8ukZcsZlEi3h8/zCObsBpJ0acqYLTmFdtqitK1V6NzrfpdPBbLmVx4uK26e27izpDu/r5yf/16AXun2Cr4u6w591xw7+LfDidLj6Mv8TXwP8xbofv/c7UmtHMmx8BAAD//0fclvU="
const FF_DEFINITION_GZIP = "H4sIAAAAAAAA/8yT327aTBDFXyU612vJxoTgvUMfKB8qcaSapqoihAZ7DNusvWi9TpUiv3tl/pdQVb1qL+cwc3bOj/EGzlKeq3T6tuaYCoZEXbGFgMogkXXDIM0y31v4C/aCgMnrU9/3gl7Pp4yilMMIAuVusqDamvlXeiWIg/FAa5OSU6aEDHz/ip4wZ5Be1AmjoBsFAtVOCO56UXh31/O7ApUjV1eQGPw3HT+NIPCitG7bctIVC2ScU63d1DK5gksHCZPnEEhXVC45rosFW8ig1++GYej3g85tJEB6aSA7Aqkpc7Ws7XahCnLTbLVM7evnzalsUUHi8//j6WgyTqYQKMilK7b31tRryLa3WKiyfRCDeHhq2Dntiys+JS/J8THUt5VyrFXlHnYTQ3LU2h91yGdQVqhy+0RtTeuhUoNZ08wagTVZdxbBndF5vYVApb7z9m9pZgKaFqwhT+6coRHvg398nEweP/157Bd+S1hz6oxtm88O73B0jbhgM47nyej+YRRfgdNODDlXJWcJL9tUF5SqnRqfbtPr4LdcTHnk4rfp3buLOkG7+Pmp++vRM9w/wVblzX7Pm8OGfxf5YDKZfxh9SS6B/2Pc9t/7ja01o5k1PwIAAP//uTipVskEAAA="

func TestParseSplitUpdate(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:         dtos.UpdateTypeSplitChange,
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
		onSplitUpdate: func(u *dtos.SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseInstantFF(t *testing.T) {
	compressType := 0
	ffDefinition := "eyJ0cmFmZmljVHlwZU5hbWUiOiJ1c2VyIiwiaWQiOiJkNDMxY2RkMC1iMGJlLTExZWEtOGE4MC0xNjYwYWRhOWNlMzkiLCJuYW1lIjoibWF1cm9famF2YSIsInRyYWZmaWNBbGxvY2F0aW9uIjoxMDAsInRyYWZmaWNBbGxvY2F0aW9uU2VlZCI6LTkyMzkxNDkxLCJzZWVkIjotMTc2OTM3NzYwNCwic3RhdHVzIjoiQUNUSVZFIiwia2lsbGVkIjpmYWxzZSwiZGVmYXVsdFRyZWF0bWVudCI6Im9mZiIsImNoYW5nZU51bWJlciI6MTY4NDMyOTg1NDM4NSwiYWxnbyI6MiwiY29uZmlndXJhdGlvbnMiOnt9LCJjb25kaXRpb25zIjpbeyJjb25kaXRpb25UeXBlIjoiV0hJVEVMSVNUIiwibWF0Y2hlckdyb3VwIjp7ImNvbWJpbmVyIjoiQU5EIiwibWF0Y2hlcnMiOlt7Im1hdGNoZXJUeXBlIjoiV0hJVEVMSVNUIiwibmVnYXRlIjpmYWxzZSwid2hpdGVsaXN0TWF0Y2hlckRhdGEiOnsid2hpdGVsaXN0IjpbImFkbWluIiwibWF1cm8iLCJuaWNvIl19fV19LCJwYXJ0aXRpb25zIjpbeyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9XSwibGFiZWwiOiJ3aGl0ZWxpc3RlZCJ9LHsiY29uZGl0aW9uVHlwZSI6IlJPTExPVVQiLCJtYXRjaGVyR3JvdXAiOnsiY29tYmluZXIiOiJBTkQiLCJtYXRjaGVycyI6W3sia2V5U2VsZWN0b3IiOnsidHJhZmZpY1R5cGUiOiJ1c2VyIn0sIm1hdGNoZXJUeXBlIjoiSU5fU0VHTUVOVCIsIm5lZ2F0ZSI6ZmFsc2UsInVzZXJEZWZpbmVkU2VnbWVudE1hdGNoZXJEYXRhIjp7InNlZ21lbnROYW1lIjoibWF1ci0yIn19XX0sInBhcnRpdGlvbnMiOlt7InRyZWF0bWVudCI6Im9uIiwic2l6ZSI6MH0seyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9LHsidHJlYXRtZW50IjoiVjQiLCJzaXplIjowfSx7InRyZWF0bWVudCI6InY1Iiwic2l6ZSI6MH1dLCJsYWJlbCI6ImluIHNlZ21lbnQgbWF1ci0yIn0seyJjb25kaXRpb25UeXBlIjoiUk9MTE9VVCIsIm1hdGNoZXJHcm91cCI6eyJjb21iaW5lciI6IkFORCIsIm1hdGNoZXJzIjpbeyJrZXlTZWxlY3RvciI6eyJ0cmFmZmljVHlwZSI6InVzZXIifSwibWF0Y2hlclR5cGUiOiJBTExfS0VZUyIsIm5lZ2F0ZSI6ZmFsc2V9XX0sInBhcnRpdGlvbnMiOlt7InRyZWF0bWVudCI6Im9uIiwic2l6ZSI6MH0seyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9LHsidHJlYXRtZW50IjoiVjQiLCJzaXplIjowfSx7InRyZWF0bWVudCI6InY1Iiwic2l6ZSI6MH1dLCJsYWJlbCI6ImRlZmF1bHQgcnVsZSJ9XX0="
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:                 dtos.UpdateTypeSplitChange,
				ChangeNumber:         123,
				PreviousChangeNumber: 1,
				CompressType:         common.IntRef(compressType),
				Definition:           common.StringRef(ffDefinition),
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
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
		onSplitUpdate: func(u *dtos.SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.FeatureFlag().ChangeNumber != 1684329854385 {
				t.Error("change number should be 1684329854385")
			}
			if u.FeatureFlag().Name != "mauro_java" {
				t.Error(FF_SHOULD_BE_MAURO_JAVA)
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseInstantFFCompressTypeZlib(t *testing.T) {
	compressType := 2
	ffDefinition := FF_DEFINITION_ZLIB
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:                 dtos.UpdateTypeSplitChange,
				ChangeNumber:         123,
				PreviousChangeNumber: 1,
				CompressType:         common.IntRef(compressType),
				Definition:           common.StringRef(ffDefinition),
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
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
		onSplitUpdate: func(u *dtos.SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.FeatureFlag().ChangeNumber != 1684265694505 {
				t.Error("change number should be 1684265694505")
			}
			if u.FeatureFlag().Name != "mauro_java" {
				t.Error(FF_SHOULD_BE_MAURO_JAVA)
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseInstantFFCompressTypeGzip(t *testing.T) {
	compressType := 1
	ffDefinition := FF_DEFINITION_GZIP
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:                 dtos.UpdateTypeSplitChange,
				ChangeNumber:         123,
				PreviousChangeNumber: 1,
				CompressType:         common.IntRef(compressType),
				Definition:           common.StringRef(ffDefinition),
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
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
		onSplitUpdate: func(u *dtos.SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.FeatureFlag().ChangeNumber != 1684333081259 {
				t.Error("change number should be 1684333081259")
			}
			if u.FeatureFlag().Name != "mauro_java" {
				t.Error(FF_SHOULD_BE_MAURO_JAVA)
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseInstantFFCompressTypeNil(t *testing.T) {
	ffDefinition := "feature flag definition"
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:                 dtos.UpdateTypeSplitChange,
				ChangeNumber:         123,
				PreviousChangeNumber: 1,
				Definition:           common.StringRef(ffDefinition),
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
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
		onSplitUpdate: func(u *dtos.SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.FeatureFlag() != nil {
				t.Error("featureFlag type should be nil")
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseInstantFFCompressTypeGreaterTwo(t *testing.T) {
	compressType := 3
	ffDefinition := "feature flga definition"
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:                 dtos.UpdateTypeSplitChange,
				ChangeNumber:         123,
				PreviousChangeNumber: 1,
				CompressType:         common.IntRef(compressType),
				Definition:           common.StringRef(ffDefinition),
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
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
		onSplitUpdate: func(u *dtos.SplitChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseSplitKill(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:             dtos.UpdateTypeSplitKill,
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
		onSplitKill: func(u *dtos.SplitKillUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_splits" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
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
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseSegmentChange(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:         dtos.UpdateTypeSegmentChange,
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
		onSegmentUpdate: func(u *dtos.SegmentChangeUpdate) error {
			if u.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, u.ChangeNumber())
			}
			if u.Channel() != "sarasa_segments" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.SegmentName() != "someSegment" {
				t.Error("segment name should be someSegment. Is: ", u.SegmentName())
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestControl(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:        dtos.UpdateTypeControl,
				ControlType: dtos.ControlTypeStreamingDisabled,
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
		onControlUpdate: func(u *dtos.ControlUpdate) *int64 {
			if u.Channel() != "control_pri" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.ControlType() != dtos.ControlTypeStreamingDisabled {
				t.Error("incorrect control type. Expected: ", u.ControlType())
			}
			return common.Int64Ref(123)
		},
	}

	if status, err := parser.ParseAndForward(event); *status != 123 || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestOccupancy(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:    dtos.SSEEventTypeMessage,
				Metrics: metrics{Publishers: 12},
			})
			mainJSON, _ := json.Marshal(genericData{
				Name:      occupancyName,
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
		onOccupancyMesage: func(u *dtos.OccupancyMessage) *int64 {
			if u.Channel() != "control_pri" {
				t.Error(CHANNEL_SHOULD_BE, u.Channel())
			}
			if u.Publishers() != 12 {
				t.Error("there should be 12 publishers. Got: ", u.Publishers())
			}
			return common.Int64Ref(123)
		},
	}

	if status, err := parser.ParseAndForward(event); *status != 123 || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestAblyError(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeError },
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
		onAblyError: func(u *dtos.AblyError) *int64 {
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
		t.Error(ERROR_SHOULD_RETURNED, err)
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
		t.Error(ERROR_SHOULD_RETURNED, err)
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
		EventCall:   func() string { return dtos.SSEEventTypeMessage },
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
		EventCall: func() string { return dtos.SSEEventTypeMessage },
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

func TestNewNotificationParserImpl(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:    dtos.SSEEventTypeMessage,
				Metrics: metrics{Publishers: 3},
			})
			mainJSON, _ := json.Marshal(genericData{
				Name:      occupancyName,
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
	parser := NewNotificationParserImpl(logger, nil, nil, nil, nil,
		func(u *dtos.OccupancyMessage) *int64 {
			if u.Channel() != "control_pri" {
				t.Error("channel should be control_pri. Is:", u.Channel())
			}
			if u.Publishers() != 3 {
				t.Error("there should be 12 publishers. Got: ", u.Publishers())
			}
			return common.Int64Ref(123)
		},
		nil,
		nil)

	if status, err := parser.ParseAndForward(event); *status != 123 || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseFFDtoNotCompress(t *testing.T) {
	compressType := 0
	ffDefinition := "eyJ0cmFmZmljVHlwZU5hbWUiOiJ1c2VyIiwiaWQiOiJkNDMxY2RkMC1iMGJlLTExZWEtOGE4MC0xNjYwYWRhOWNlMzkiLCJuYW1lIjoibWF1cm9famF2YSIsInRyYWZmaWNBbGxvY2F0aW9uIjoxMDAsInRyYWZmaWNBbGxvY2F0aW9uU2VlZCI6LTkyMzkxNDkxLCJzZWVkIjotMTc2OTM3NzYwNCwic3RhdHVzIjoiQUNUSVZFIiwia2lsbGVkIjpmYWxzZSwiZGVmYXVsdFRyZWF0bWVudCI6Im9mZiIsImNoYW5nZU51bWJlciI6MTY4NDMyOTg1NDM4NSwiYWxnbyI6MiwiY29uZmlndXJhdGlvbnMiOnt9LCJjb25kaXRpb25zIjpbeyJjb25kaXRpb25UeXBlIjoiV0hJVEVMSVNUIiwibWF0Y2hlckdyb3VwIjp7ImNvbWJpbmVyIjoiQU5EIiwibWF0Y2hlcnMiOlt7Im1hdGNoZXJUeXBlIjoiV0hJVEVMSVNUIiwibmVnYXRlIjpmYWxzZSwid2hpdGVsaXN0TWF0Y2hlckRhdGEiOnsid2hpdGVsaXN0IjpbImFkbWluIiwibWF1cm8iLCJuaWNvIl19fV19LCJwYXJ0aXRpb25zIjpbeyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9XSwibGFiZWwiOiJ3aGl0ZWxpc3RlZCJ9LHsiY29uZGl0aW9uVHlwZSI6IlJPTExPVVQiLCJtYXRjaGVyR3JvdXAiOnsiY29tYmluZXIiOiJBTkQiLCJtYXRjaGVycyI6W3sia2V5U2VsZWN0b3IiOnsidHJhZmZpY1R5cGUiOiJ1c2VyIn0sIm1hdGNoZXJUeXBlIjoiSU5fU0VHTUVOVCIsIm5lZ2F0ZSI6ZmFsc2UsInVzZXJEZWZpbmVkU2VnbWVudE1hdGNoZXJEYXRhIjp7InNlZ21lbnROYW1lIjoibWF1ci0yIn19XX0sInBhcnRpdGlvbnMiOlt7InRyZWF0bWVudCI6Im9uIiwic2l6ZSI6MH0seyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9LHsidHJlYXRtZW50IjoiVjQiLCJzaXplIjowfSx7InRyZWF0bWVudCI6InY1Iiwic2l6ZSI6MH1dLCJsYWJlbCI6ImluIHNlZ21lbnQgbWF1ci0yIn0seyJjb25kaXRpb25UeXBlIjoiUk9MTE9VVCIsIm1hdGNoZXJHcm91cCI6eyJjb21iaW5lciI6IkFORCIsIm1hdGNoZXJzIjpbeyJrZXlTZWxlY3RvciI6eyJ0cmFmZmljVHlwZSI6InVzZXIifSwibWF0Y2hlclR5cGUiOiJBTExfS0VZUyIsIm5lZ2F0ZSI6ZmFsc2V9XX0sInBhcnRpdGlvbnMiOlt7InRyZWF0bWVudCI6Im9uIiwic2l6ZSI6MH0seyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9LHsidHJlYXRtZW50IjoiVjQiLCJzaXplIjowfSx7InRyZWF0bWVudCI6InY1Iiwic2l6ZSI6MH1dLCJsYWJlbCI6ImRlZmF1bHQgcnVsZSJ9XX0="
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto == nil {
		t.Error(FF_NOT_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoNotCompressWrongDefinition(t *testing.T) {
	compressType := 0
	ffDefinition := "eyJ0cmFmZmldfsfsfjVHlwZU5hbWUiOiJ1c2VyIiwiaWQiOiJkNDMxY2RkMC1iMGJlLTExZWEtOGE4MC0xNjYwYWRhOWNlMzkiLCJuYW1lIjoibWF1cm9famF2YSIsInRyYWZmaWNBbGxvY2F0aW9uIjoxMDAsInRyYWZmaWNBbGxvY2F0aW9uU2VlZCI6LTkyMzkxNDkxLCJzZWVkIjotMTc2OTM3NzYwNCwic3RhdHVzIjoiQUNUSVZFIiwia2lsbGVkIjpmYWxzZSwiZGVmYXVsdFRyZWF0bWVudCI6Im9mZiIsImNoYW5nZU51bWJlciI6MTY4NDMyOTg1NDM4NSwiYWxnbyI6MiwiY29uZmlndXJhdGlvbnMiOnt9LCJjb25kaXRpb25zIjpbeyJjb25kaXRpb25UeXBlIjoiV0hJVEVMSVNUIiwibWF0Y2hlckdyb3VwIjp7ImNvbWJpbmVyIjoiQU5EIiwibWF0Y2hlcnMiOlt7Im1hdGNoZXJUeXBlIjoiV0hJVEVMSVNUIiwibmVnYXRlIjpmYWxzZSwid2hpdGVsaXN0TWF0Y2hlckRhdGEiOnsid2hpdGVsaXN0IjpbImFkbWluIiwibWF1cm8iLCJuaWNvIl19fV19LCJwYXJ0aXRpb25zIjpbeyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9XSwibGFiZWwiOiJ3aGl0ZWxpc3RlZCJ9LHsiY29uZGl0aW9uVHlwZSI6IlJPTExPVVQiLCJtYXRjaGVyR3JvdXAiOnsiY29tYmluZXIiOiJBTkQiLCJtYXRjaGVycyI6W3sia2V5U2VsZWN0b3IiOnsidHJhZmZpY1R5cGUiOiJ1c2VyIn0sIm1hdGNoZXJUeXBlIjoiSU5fU0VHTUVOVCIsIm5lZ2F0ZSI6ZmFsc2UsInVzZXJEZWZpbmVkU2VnbWVudE1hdGNoZXJEYXRhIjp7InNlZ21lbnROYW1lIjoibWF1ci0yIn19XX0sInBhcnRpdGlvbnMiOlt7InRyZWF0bWVudCI6Im9uIiwic2l6ZSI6MH0seyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9LHsidHJlYXRtZW50IjoiVjQiLCJzaXplIjowfSx7InRyZWF0bWVudCI6InY1Iiwic2l6ZSI6MH1dLCJsYWJlbCI6ImluIHNlZ21lbnQgbWF1ci0yIn0seyJjb25kaXRpb25UeXBlIjoiUk9MTE9VVCIsIm1hdGNoZXJHcm91cCI6eyJjb21iaW5lciI6IkFORCIsIm1hdGNoZXJzIjpbeyJrZXlTZWxlY3RvciI6eyJ0cmFmZmljVHlwZSI6InVzZXIifSwibWF0Y2hlclR5cGUiOiJBTExfS0VZUyIsIm5lZ2F0ZSI6ZmFsc2V9XX0sInBhcnRpdGlvbnMiOlt7InRyZWF0bWVudCI6Im9uIiwic2l6ZSI6MH0seyJ0cmVhdG1lbnQiOiJvZmYiLCJzaXplIjoxMDB9LHsidHJlYXRtZW50IjoiVjQiLCJzaXplIjowfSx7InRyZWF0bWVudCI6InY1Iiwic2l6ZSI6MH1dLCJsYWJlbCI6ImRlZmF1bHQgcnVsZSJ9XX0="
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto != nil {
		t.Error(FF_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoGzipCompress(t *testing.T) {
	compressType := 1
	ffDefinition := FF_DEFINITION_GZIP
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto == nil {
		t.Error(FF_NOT_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoZlibCompressWrongCompressType(t *testing.T) {
	compressType := 2
	ffDefinition := FF_DEFINITION_GZIP
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto != nil {
		t.Error(FF_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoZlibCompress(t *testing.T) {
	compressType := 2
	ffDefinition := FF_DEFINITION_ZLIB
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto == nil {
		t.Error(FF_NOT_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoGzipCompressWrongDefinition(t *testing.T) {
	compressType := 1
	ffDefinition := FF_DEFINITION_ZLIB
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto != nil {
		t.Error(FF_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoCompressTypeNil(t *testing.T) {
	ffDefinition := FF_DEFINITION_ZLIB
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		Definition:           common.StringRef(ffDefinition),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto != nil {
		t.Error(FF_SHOULD_BE_NIL)
	}
}

func TestParseFFDtoDefinitionNil(t *testing.T) {
	compressType := 1
	data := genericMessageData{
		Type:                 dtos.UpdateTypeSplitChange,
		ChangeNumber:         123,
		PreviousChangeNumber: 1,
		CompressType:         common.IntRef(compressType),
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		dataUtils: NewDataUtilsImpl(),
		logger:    logger,
	}
	ffDto := parser.processMessage(&data)
	if ffDto != nil {
		t.Error(FF_SHOULD_BE_NIL)
	}
}

func TestParseLargeSegmentChange(t *testing.T) {
	lsData := "{\"type\":\"LS_DEFINITION_UPDATE\",\"changeNumber\":123,\"ls\":[{\"n\":\"lsNameTest\",\"rfd\":{\"p\":{\"m\":\"GET\",\"u\":\"https://split-large-segments.com\",\"h\":{\"Host\":[\"split-large\"]}},\"d\":{\"k\":10,\"s\":56,\"f\":1,\"e\":1732230421086,\"v\":\"1.0\"}},\"t\":\"LS_NEW_DEFINITION\"}]}"
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      lsData,
				Channel:   "largesegments_channel",
			})
			return string(mainJSON)
		},
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onLargeSegmentUpdate: func(lscu *dtos.LargeSegmentChangeUpdate) error {
			if lscu.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, lscu.ChangeNumber())
			}
			if lscu.Channel() != "largesegments_channel" {
				t.Error(CHANNEL_SHOULD_BE, lscu.Channel())
			}
			if len(lscu.LargeSegments) != 1 {
				t.Error("Large Segments len should be 1. Actual: ", len(lscu.LargeSegments))
			}
			ls := lscu.LargeSegments[0]
			if ls.Name != "lsNameTest" {
				t.Error("LS Name should be lsNameTest. Actual: ", ls.Name)
			}
			if ls.RFD.Data.Format != 1 {
				t.Error("Unexpected data format. Actual: ", ls.RFD.Data.Format)
			}
			if ls.NotificationType != "LS_NEW_DEFINITION" {
				t.Error("Unexpected Notification Type: Actual: ", ls.NotificationType)
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseLargeSegmentChangeNestedMessage(t *testing.T) {
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			updateJSON, _ := json.Marshal(genericMessageData{
				Type:          dtos.UpdateTypeLargeSegmentChange,
				ChangeNumber:  123,
				LargeSegments: nil,
			})
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      string(updateJSON),
				Channel:   "largesegments_channel",
			})
			return string(mainJSON)
		},
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onLargeSegmentUpdate: func(lscu *dtos.LargeSegmentChangeUpdate) error {
			if lscu.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, lscu.ChangeNumber())
			}
			if lscu.Channel() != "largesegments_channel" {
				t.Error(CHANNEL_SHOULD_BE, lscu.Channel())
			}
			if len(lscu.LargeSegments) != 0 {
				t.Error("Large Segments len should be 0. Actual: ", len(lscu.LargeSegments))
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestParseLargeSegmentChangeWithEmptyList(t *testing.T) {
	lsData := "{\"type\":\"LS_DEFINITION_UPDATE\",\"changeNumber\":1732227257338,\"ls\":[{\"n\":\"maldo_ls\",\"t\":\"LS_EMPTY\"}]}"
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      lsData,
				Channel:   "largesegments_channel",
			})
			return string(mainJSON)
		},
	}
	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onLargeSegmentUpdate: func(lscu *dtos.LargeSegmentChangeUpdate) error {
			if lscu.ChangeNumber() != 1732227257338 {
				t.Error(CN_SHOULD_BE_123, lscu.ChangeNumber())
			}
			if lscu.Channel() != "largesegments_channel" {
				t.Error(CHANNEL_SHOULD_BE, lscu.Channel())
			}
			if len(lscu.LargeSegments) != 1 {
				t.Error("Large Segments len should be 1. Actual: ", len(lscu.LargeSegments))
			}
			ls := lscu.LargeSegments[0]
			if ls.Name != "maldo_ls" {
				t.Error("Unexpected LS Name: Actual: ", ls.Name)
			}
			if ls.NotificationType != "LS_EMPTY" {
				t.Error("Unexpected Notification Type: Actual: ", ls.NotificationType)
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}

func TestLargeParseSegmentChangeWrongLsDefinition(t *testing.T) {
	lsData := "{\"type\":\"LS_DEFINITION_UPDATE\",\"changeNumber\":123,\"ls\":[]}"
	event := &sseMocks.RawEventMock{
		IDCall:    func() string { return "abc" },
		EventCall: func() string { return dtos.SSEEventTypeMessage },
		DataCall: func() string {
			mainJSON, _ := json.Marshal(genericData{
				Timestamp: 123,
				Data:      lsData,
				Channel:   "largesegments_channel",
			})
			return string(mainJSON)
		},
	}

	logger := logging.NewLogger(nil)
	parser := &NotificationParserImpl{
		logger: logger,
		onLargeSegmentUpdate: func(lscu *dtos.LargeSegmentChangeUpdate) error {
			if lscu.ChangeNumber() != 123 {
				t.Error(CN_SHOULD_BE_123, lscu.ChangeNumber())
			}
			if lscu.Channel() != "largesegments_channel" {
				t.Error(CHANNEL_SHOULD_BE, lscu.Channel())
			}
			if len(lscu.LargeSegments) != 0 {
				t.Error("Large Segments len should be 0. Actual: ", len(lscu.LargeSegments))
			}
			return nil
		},
	}

	if status, err := parser.ParseAndForward(event); status != nil || err != nil {
		t.Error(ERROR_SHOULD_RETURNED, err)
	}
}
