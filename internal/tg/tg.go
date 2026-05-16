package tg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	token  string
	client *http.Client
}

type Update struct {
	UpdateID int64   `json:"update_id"`
	Message  Message `json:"message"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	Chat      Chat   `json:"chat"`
	From      User   `json:"from"`
	Text      string `json:"text"`
}

type Chat struct {
	ID int64 `json:"id"`
}

type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

func New(token string) *Client {
	return &Client{
		token:  token,
		client: &http.Client{Timeout: 70 * time.Second},
	}
}

func (c *Client) GetUpdates(ctx context.Context, offset int64) ([]Update, error) {
	values := url.Values{}
	values.Set("timeout", "60")
	if offset > 0 {
		values.Set("offset", strconv.FormatInt(offset, 10))
	}

	var decoded struct {
		OK     bool     `json:"ok"`
		Result []Update `json:"result"`
	}
	if err := c.call(ctx, http.MethodGet, "getUpdates", values, &decoded); err != nil {
		return nil, err
	}
	if !decoded.OK {
		return nil, fmt.Errorf("telegram returned ok=false")
	}
	return decoded.Result, nil
}

func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("text", text)

	return c.sendMessage(ctx, values)
}

func (c *Client) SendHTML(ctx context.Context, chatID int64, text string) error {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("text", text)
	values.Set("parse_mode", "HTML")

	return c.sendMessage(ctx, values)
}

func (c *Client) SendReply(ctx context.Context, chatID, replyToID int64, text string) error {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("reply_to_message_id", strconv.FormatInt(replyToID, 10))
	values.Set("text", text)

	return c.sendMessage(ctx, values)
}

func (c *Client) SendHTMLReply(ctx context.Context, chatID, replyToID int64, text string) error {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("reply_to_message_id", strconv.FormatInt(replyToID, 10))
	values.Set("text", text)
	values.Set("parse_mode", "HTML")

	return c.sendMessage(ctx, values)
}

func (c *Client) sendMessage(ctx context.Context, values url.Values) error {
	var decoded struct {
		OK bool `json:"ok"`
	}
	return c.call(ctx, http.MethodPost, "sendMessage", values, &decoded)
}

func (c *Client) call(ctx context.Context, method, apiMethod string, values url.Values, out any) error {
	endpoint := "https://api.telegram.org/bot" + c.token + "/" + apiMethod
	var body io.Reader
	if method == http.MethodGet && len(values) > 0 {
		endpoint += "?" + values.Encode()
	} else if len(values) > 0 {
		body = strings.NewReader(values.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram status %s: %s", resp.Status, string(data))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}
