package max

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func withServer(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	old := apiBase
	apiBase = server.URL
	t.Cleanup(func() { apiBase = old })
	return New("secret")
}

func TestGetUpdatesTracksMarker(t *testing.T) {
	var calls int
	client := withServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "secret" {
			t.Fatal("missing authorization")
		}
		calls++
		if calls == 2 && r.URL.Query().Get("marker") != "42" {
			t.Fatalf("marker = %q, want 42", r.URL.Query().Get("marker"))
		}
		_, _ = io.WriteString(w, `{"updates":[],"marker":42}`)
	})
	if _, err := client.GetUpdates(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetUpdates(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDrainUsesNonblockingPoll(t *testing.T) {
	client := withServer(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("timeout"); got != "0" {
			t.Fatalf("timeout = %q, want 0", got)
		}
		_, _ = io.WriteString(w, `{"updates":[],"marker":"next"}`)
	})
	if err := client.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSendAndEdit(t *testing.T) {
	var requests int
	client := withServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["text"] != "hello" || body["format"] != "html" {
			t.Fatalf("body = %#v", body)
		}
		if r.Method == http.MethodPost {
			if r.URL.Query().Get("user_id") != "7" {
				t.Fatalf("user_id = %q", r.URL.Query().Get("user_id"))
			}
			if body["link"].(map[string]any)["mid"] != "source" {
				t.Fatalf("link = %#v", body["link"])
			}
			_, _ = io.WriteString(w, `{"message":{"body":{"mid":"sent"}}}`)
			return
		}
		if r.Method != http.MethodPut || r.URL.Query().Get("message_id") != "sent" {
			t.Fatalf("edit request = %s %s", r.Method, r.URL.String())
		}
		_, _ = io.WriteString(w, `{}`)
	})
	id, err := client.Send(context.Background(), "7", "source", "hello", "html")
	if err != nil || id != "sent" {
		t.Fatalf("Send = %q, %v", id, err)
	}
	if err := client.Edit(context.Background(), "7", id, "hello", "html"); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}
