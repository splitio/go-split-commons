package redis

import (
	"errors"
	"strings"
)

// Public errors
var (
	ErrChangeNumberUpdateFailed = errors.New("failed to update change number")
)

// UpdateError contains information on what splits failed to be added/removed and why
type UpdateError struct {
	FailedToAdd    map[string]error
	FailedToRemove map[string]error
}

func (u *UpdateError) Error() string {
	builder := strings.Builder{}
	if len(u.FailedToAdd) > 0 {
		builder.WriteString("failed to add the following splits [" + formatMapKeys(u.FailedToAdd) + "]")
		if len(u.FailedToRemove) > 0 {
			builder.WriteString(" and ")
		}
	}
	if len(u.FailedToRemove) > 0 {
		builder.WriteString("failed to remove the following splits [" + formatMapKeys(u.FailedToRemove) + "]")
	}

	return builder.String()
}

func formatMapKeys(in map[string]error) string {
	slice := make([]string, 0, len(in))
	for key := range in {
		slice = append(slice, key)
	}
	return strings.Join(slice, ",")
}

var _ error = (*UpdateError)(nil)
