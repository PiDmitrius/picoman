package transport

import (
	"context"
	"fmt"
)

type Address struct {
	Transport string
	ChatID    string
}

type Message struct {
	Address   Address
	MessageID string
	SenderID  int64
	Username  string
	Text      string
}

type Client interface {
	Send(context.Context, string, string, string, string) (string, error)
	Edit(context.Context, string, string, string, string) error
}

type APIError struct {
	Platform    string
	Code        int
	Description string
	RetryAfter  int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s %d: %s", e.Platform, e.Code, e.Description)
}

func (e *APIError) IsRetryable() bool {
	return e.RetryAfter > 0 || e.Code == 429 || e.Code >= 500
}
