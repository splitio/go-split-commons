package dtos

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// Token dto
type Token struct {
	Token       string `json:"token"`
	PushEnabled bool   `json:"pushEnabled"`
}

// TokenPayload payload dto
type TokenPayload struct {
	Capabilitites string `json:"x-ably-capability"`
}

// ParsedCapabilities capabilities
type ParsedCapabilities map[string][]string

// ChannelList grabs the channel list from capabilities
func (t *Token) ChannelList() ([]string, error) {
	if !t.PushEnabled || t.Token == "" {
		return nil, errors.New("Push disabled or no token set")
	}

	tokenParts := strings.Split(t.Token, ".")
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
		channelList = append(channelList, channelName)
	}

	return channelList, nil
}
