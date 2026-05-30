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
	_, err := c.SendMessageResult(ctx, chatID, text)
	return err
}

func (c *Client) SendMessageResult(ctx context.Context, chatID int64, text string) (Message, error) {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("text", text)

	return c.sendMessage(ctx, values)
}

func (c *Client) SendHTML(ctx context.Context, chatID int64, text string) error {
	_, err := c.SendHTMLResult(ctx, chatID, text)
	return err
}

func (c *Client) SendHTMLResult(ctx context.Context, chatID int64, text string) (Message, error) {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("text", text)
	values.Set("parse_mode", "HTML")

	return c.sendMessage(ctx, values)
}

func (c *Client) SendReply(ctx context.Context, chatID, replyToID int64, text string) error {
	_, err := c.SendReplyResult(ctx, chatID, replyToID, text)
	return err
}

func (c *Client) SendReplyResult(ctx context.Context, chatID, replyToID int64, text string) (Message, error) {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("reply_to_message_id", strconv.FormatInt(replyToID, 10))
	values.Set("text", text)

	return c.sendMessage(ctx, values)
}

func (c *Client) SendHTMLReply(ctx context.Context, chatID, replyToID int64, text string) error {
	_, err := c.SendHTMLReplyResult(ctx, chatID, replyToID, text)
	return err
}

func (c *Client) SendHTMLReplyResult(ctx context.Context, chatID, replyToID int64, text string) (Message, error) {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("reply_to_message_id", strconv.FormatInt(replyToID, 10))
	values.Set("text", text)
	values.Set("parse_mode", "HTML")

	return c.sendMessage(ctx, values)
}

func (c *Client) sendMessage(ctx context.Context, values url.Values) (Message, error) {
	var decoded struct {
		OK     bool    `json:"ok"`
		Result Message `json:"result"`
	}
	if err := c.call(ctx, http.MethodPost, "sendMessage", values, &decoded); err != nil {
		return Message{}, err
	}
	if !decoded.OK {
		return Message{}, fmt.Errorf("telegram returned ok=false")
	}
	return decoded.Result, nil
}

func (c *Client) EditMessage(ctx context.Context, chatID, messageID int64, text string) error {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("message_id", strconv.FormatInt(messageID, 10))
	values.Set("text", text)

	return c.editMessage(ctx, values)
}

func (c *Client) EditHTMLMessage(ctx context.Context, chatID, messageID int64, text string) error {
	values := url.Values{}
	values.Set("chat_id", strconv.FormatInt(chatID, 10))
	values.Set("message_id", strconv.FormatInt(messageID, 10))
	values.Set("text", text)
	values.Set("parse_mode", "HTML")

	return c.editMessage(ctx, values)
}

func (c *Client) editMessage(ctx context.Context, values url.Values) error {
	var decoded struct {
		OK bool `json:"ok"`
	}
	if err := c.call(ctx, http.MethodPost, "editMessageText", values, &decoded); err != nil {
		return err
	}
	if !decoded.OK {
		return fmt.Errorf("telegram returned ok=false")
	}
	return nil
}

// APIError is a Telegram API error from a non-2xx response.
// Network and transport errors are returned as-is from c.client.Do and are
// not wrapped in APIError, so callers can distinguish them with errors.As.
type APIError struct {
	Code        int
	Description string
	RetryAfter  int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram %d: %s", e.Code, e.Description)
}

func (e *APIError) IsRetryable() bool {
	return e.RetryAfter > 0 || e.Code == 429 || e.Code >= 500
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
		return redactToken(err, c.token)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return redactToken(err, c.token)
	}
	if resp.StatusCode != http.StatusOK {
		var decoded struct {
			ErrorCode   int    `json:"error_code"`
			Description string `json:"description"`
			Parameters  struct {
				RetryAfter int `json:"retry_after"`
			} `json:"parameters"`
		}
		_ = json.Unmarshal(data, &decoded)
		code := decoded.ErrorCode
		if code == 0 {
			code = resp.StatusCode
		}
		return &APIError{
			Code:        code,
			Description: decoded.Description,
			RetryAfter:  decoded.Parameters.RetryAfter,
		}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

// redactToken strips the bot token from net/url errors so it does not leak
// into logs or persistent stores.
func redactToken(err error, token string) error {
	if err == nil || token == "" {
		return err
	}
	msg := err.Error()
	if !strings.Contains(msg, token) {
		return err
	}
	return fmt.Errorf("%s", strings.ReplaceAll(msg, token, "<redacted>"))
}

// BotCommand is one entry in the Telegram command menu.
type BotCommand struct {
	Command     string `json:"command"`
	Description string `json:"description"`
}

// SetMyCommands sets the bot command menu shown to users.
func (c *Client) SetMyCommands(ctx context.Context, commands []BotCommand) error {
	data, err := json.Marshal(commands)
	if err != nil {
		return err
	}
	values := url.Values{}
	values.Set("commands", string(data))
	var decoded struct {
		OK bool `json:"ok"`
	}
	if err := c.call(ctx, http.MethodPost, "setMyCommands", values, &decoded); err != nil {
		return err
	}
	if !decoded.OK {
		return fmt.Errorf("telegram returned ok=false")
	}
	return nil
}
