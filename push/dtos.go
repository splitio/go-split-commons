package push

// IncomingEvent struct to process every kind of notification that comes from streaming
type IncomingEvent struct {
	id         *string
	timestamp  *int64
	encoding   *string
	data       *string
	name       *string
	clientID   *string
	event      string
	channel    *string
	message    *string
	code       *int64
	statusCode *int64
	href       *string
}
