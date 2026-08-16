package tg

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestMessagesDisableWebPagePreview(t *testing.T) {
	var requests []url.Values
	client := New("token")
	client.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request.PostForm)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":1}}`)),
			Header:     make(http.Header),
		}, nil
	})

	if _, err := client.SendMessageResult(context.Background(), 1, "https://example.com"); err != nil {
		t.Fatal(err)
	}
	if err := client.EditMessage(context.Background(), 1, 1, "https://example.com"); err != nil {
		t.Fatal(err)
	}
	for i, values := range requests {
		if got := values.Get("disable_web_page_preview"); got != "true" {
			t.Fatalf("request %d disable_web_page_preview = %q", i, got)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
