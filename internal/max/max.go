package max

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"picoman/internal/transport"
)

var apiBase = "https://platform-api2.max.ru"

type Client struct {
	token  string
	http   *http.Client
	marker string
	mu     sync.Mutex
}

type User struct {
	ID       int64  `json:"user_id"`
	Name     string `json:"name"`
	Username string `json:"username"`
}

type Update struct {
	Type    string `json:"update_type"`
	Message struct {
		Sender    User `json:"sender"`
		Recipient struct {
			ChatID   int64  `json:"chat_id"`
			ChatType string `json:"chat_type"`
		} `json:"recipient"`
		Body struct {
			ID   string `json:"mid"`
			Text string `json:"text"`
		} `json:"body"`
	} `json:"message"`
}

func New(token string) *Client {
	return &Client{token: token, http: newMaxHTTPClient()}
}

func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

func (c *Client) GetMe(ctx context.Context) (User, error) {
	resp, err := c.request(ctx, http.MethodGet, "/me", nil)
	if err != nil {
		return User{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return User{}, apiError(resp, "GET /me")
	}
	var user User
	return user, json.NewDecoder(resp.Body).Decode(&user)
}

func (c *Client) GetUpdates(ctx context.Context) ([]Update, error) {
	return c.getUpdates(ctx, "30")
}

func (c *Client) getUpdates(ctx context.Context, timeout string) ([]Update, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	q := url.Values{"timeout": {timeout}, "types": {"message_created"}}
	if c.marker != "" {
		q.Set("marker", c.marker)
	}
	resp, err := c.request(ctx, http.MethodGet, "/updates?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp, "GET /updates")
	}
	var result struct {
		Updates []Update        `json:"updates"`
		Marker  json.RawMessage `json:"marker"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Marker) > 0 && string(result.Marker) != "null" {
		if err := json.Unmarshal(result.Marker, &c.marker); err != nil {
			var numeric int64
			if err := json.Unmarshal(result.Marker, &numeric); err != nil {
				return nil, err
			}
			c.marker = strconv.FormatInt(numeric, 10)
		}
	}
	return result.Updates, nil
}

func (c *Client) Drain(ctx context.Context) error {
	_, err := c.getUpdates(ctx, "0")
	return err
}

func (c *Client) Send(ctx context.Context, chatID, replyTo, text, format string) (string, error) {
	payload := map[string]any{"text": text}
	if format == "html" {
		payload["format"] = "html"
	}
	if replyTo != "" {
		payload["link"] = map[string]string{"type": "reply", "mid": replyTo}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	query := "chat_id=" + url.QueryEscape(chatID)
	if id, err := strconv.ParseInt(chatID, 10, 64); err == nil && id > 0 {
		query = "user_id=" + url.QueryEscape(chatID)
	}
	resp, err := c.request(ctx, http.MethodPost, "/messages?"+query, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", apiError(resp, "POST /messages")
	}
	var result struct {
		Message struct {
			Body struct {
				ID string `json:"mid"`
			} `json:"body"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.Message.Body.ID, nil
}

func (c *Client) Edit(ctx context.Context, _ string, messageID, text, format string) error {
	payload := map[string]any{"text": text}
	if format == "html" {
		payload["format"] = "html"
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := c.request(ctx, http.MethodPut, "/messages?message_id="+url.QueryEscape(messageID), bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return apiError(resp, "PUT /messages")
	}
	return nil
}

func apiError(resp *http.Response, operation string) error {
	data, _ := io.ReadAll(resp.Body)
	return &transport.APIError{Platform: "max", Code: resp.StatusCode, Description: operation + ": " + string(data)}
}
