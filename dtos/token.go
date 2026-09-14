package dtos

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const gracePeriod = 10 * time.Minute
const metadataPlaceHolder = "channel-metadata:publishers"
const occupancy = "[?occupancy=metrics.publishers]"

// Token dto
type Token struct {
	Token       string `json:"token"`
	PushEnabled bool   `json:"pushEnabled"`
}

// UnmarshalJSON parses the legacy `pushEnabled` boolean when present, and otherwise falls back to
// `config.streaming.enabled` - some auth backends (e.g. the Configs auth service) have moved
// streaming capability there instead of the top-level field. When neither is present, PushEnabled
// defaults to false, matching the pre-existing zero-value behavior.
func (t *Token) UnmarshalJSON(raw []byte) error {
	var shadow struct {
		Token       string `json:"token"`
		PushEnabled *bool  `json:"pushEnabled"`
		Config      *struct {
			Streaming *struct {
				Enabled *bool `json:"enabled"`
			} `json:"streaming"`
		} `json:"config"`
	}
	if err := json.Unmarshal(raw, &shadow); err != nil {
		return err
	}

	t.Token = shadow.Token
	switch {
	case shadow.PushEnabled != nil:
		t.PushEnabled = *shadow.PushEnabled
	case shadow.Config != nil && shadow.Config.Streaming != nil && shadow.Config.Streaming.Enabled != nil:
		t.PushEnabled = *shadow.Config.Streaming.Enabled
	default:
		t.PushEnabled = false
	}
	return nil
}

// TokenPayload payload dto
type TokenPayload struct {
	Capabilitites string `json:"x-ably-capability"`
	Exp           int64  `json:"exp"`
	Iat           int64  `json:"iat"`
}

// ParsedCapabilities capabilities
type ParsedCapabilities map[string][]string

func isMetadataType(capabilities []string) bool {
	for _, capability := range capabilities {
		if capability == metadataPlaceHolder {
			return true
		}
	}
	return false
}

// ChannelList grabs the channel list from capabilities
func (t *Token) ChannelList() ([]string, error) {
	if !t.PushEnabled || t.Token == "" {
		return nil, errors.New("Push disabled or no token set")
	}

	tokenParts := strings.Split(t.Token, ".")
	if len(tokenParts) < 2 {
		return nil, errors.New("Cannot decode token")
	}
	decodedPayload, err := base64.RawURLEncoding.DecodeString(tokenParts[1])
	if err != nil {
		return nil, err
	}

	var parsedPayload TokenPayload
	err = json.Unmarshal(decodedPayload, &parsedPayload)
	if err != nil {
		return nil, err
	}

	var parsedCapabilities ParsedCapabilities
	err = json.Unmarshal([]byte(parsedPayload.Capabilitites), &parsedCapabilities)
	if err != nil {
		return nil, err
	}

	channelList := make([]string, 0, len(parsedCapabilities))
	for channelName := range parsedCapabilities {
		if isMetadataType(parsedCapabilities[channelName]) {
			channelList = append(channelList, fmt.Sprintf("%s%s", occupancy, channelName))
		} else {
			channelList = append(channelList, channelName)
		}
	}

	return channelList, nil
}

// CalculateNextTokenExpiration calculates next token expiration
func (t *Token) CalculateNextTokenExpiration() (time.Duration, error) {
	if !t.PushEnabled || t.Token == "" {
		return 0, errors.New("Push disabled or no token set")
	}

	tokenParts := strings.Split(t.Token, ".")
	if len(tokenParts) < 2 {
		return 0, errors.New("Cannot decode token")
	}
	decodedPayload, err := base64.RawURLEncoding.DecodeString(tokenParts[1])
	if err != nil {
		return 0, err
	}

	var parsedPayload TokenPayload
	err = json.Unmarshal(decodedPayload, &parsedPayload)
	if err != nil {
		return 0, err
	}

	tokenDuration := parsedPayload.Exp - parsedPayload.Iat
	return time.Duration(tokenDuration)*time.Second - gracePeriod, nil
}
