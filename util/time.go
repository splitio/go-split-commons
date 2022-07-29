package util

const dedupWindowSizeMs = 3600 * 1000

// TruncateTimeFrame truncates de time frame received with the time window
func TruncateTimeFrame(timestampInMs int64) int64 {
	return timestampInMs - (timestampInMs % dedupWindowSizeMs)
}
