package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	_ "github.com/mattn/go-sqlite3"

	"picoman/internal/tg"
)

const (
	maxAttempts = 5
	baseBackoff = time.Second
	maxBackoff  = time.Minute
)

type Store struct {
	db  *sql.DB
	bot *tg.Client
}

type message struct {
	id        int64
	chatID    int64
	replyToID int64
	format    string
	text      string
	attempts  int
}

func Open(path string, bot *tg.Client) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, bot: bot}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	_ = os.Chmod(path, 0o600)
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Enqueue(chatID int64, text string) error {
	return s.enqueue(chatID, 0, "", text)
}

func (s *Store) EnqueueReply(chatID, replyToID int64, text string) error {
	return s.enqueue(chatID, replyToID, "", text)
}

func (s *Store) EnqueueHTML(chatID int64, text string) error {
	return s.enqueue(chatID, 0, "html", text)
}

func (s *Store) EnqueueHTMLReply(chatID, replyToID int64, text string) error {
	return s.enqueue(chatID, replyToID, "html", text)
}

func (s *Store) enqueue(chatID, replyToID int64, format, text string) error {
	chunks := splitMessage(text, 4000, format)
	for i, chunk := range chunks {
		id := int64(0)
		if i == 0 {
			id = replyToID
		}
		if err := s.insert(id, chatID, format, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) insert(replyToID, chatID int64, format, text string) error {
	_, err := s.db.Exec(`
insert into outbox(chat_id, reply_to_id, format, text, created_at)
values(?, ?, ?, ?, unixepoch())
`, chatID, replyToID, format, text)
	return err
}

func (s *Store) Run(ctx context.Context) {
	wait := baseBackoff
	for {
		err := s.SendOne(ctx)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			wait = baseBackoff
			if !sleepCtx(ctx, time.Second) {
				return
			}
		case err == nil:
			wait = baseBackoff
		default:
			if !sleepCtx(ctx, wait) {
				return
			}
			wait *= 2
			if wait > maxBackoff {
				wait = maxBackoff
			}
		}
	}
}

func (s *Store) Flush(ctx context.Context) {
	for ctx.Err() == nil {
		err := s.SendOne(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		if err != nil && !sleepCtx(ctx, time.Second) {
			return
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Store) SendOne(ctx context.Context) error {
	msg, err := s.next()
	if err != nil {
		return err
	}
	sendErr := s.send(ctx, msg)
	if sendErr == nil {
		log.Printf("outbox sent id=%d chat=%d", msg.id, msg.chatID)
		return s.markSent(msg.id)
	}

	// Reply target gone: drop reply_to and retry on next tick.
	if msg.replyToID > 0 && isReplyTargetGone(sendErr) {
		log.Printf("outbox reply target gone id=%d chat=%d: %v", msg.id, msg.chatID, sendErr)
		_ = s.clearReplyTo(msg.id)
		return sendErr
	}

	var apiErr *tg.APIError
	permanent := errors.As(sendErr, &apiErr) && !apiErr.IsRetryable()
	if !permanent {
		s.recordError(msg.id, sendErr)
		log.Printf("outbox transient id=%d chat=%d: %v", msg.id, msg.chatID, sendErr)
		return sendErr
	}

	attempts := msg.attempts + 1
	s.fail(msg.id, attempts, sendErr)
	if attempts >= maxAttempts {
		s.markDead(msg.id)
		log.Printf("outbox dead id=%d chat=%d attempts=%d: %v", msg.id, msg.chatID, attempts, sendErr)
		go s.notifyDead(msg, sendErr)
		return nil
	}
	log.Printf("outbox permanent id=%d chat=%d attempts=%d: %v", msg.id, msg.chatID, attempts, sendErr)
	return sendErr
}

func (s *Store) send(ctx context.Context, msg message) error {
	switch {
	case msg.format == "html" && msg.replyToID > 0:
		return s.bot.SendHTMLReply(ctx, msg.chatID, msg.replyToID, msg.text)
	case msg.format == "html":
		return s.bot.SendHTML(ctx, msg.chatID, msg.text)
	case msg.replyToID > 0:
		return s.bot.SendReply(ctx, msg.chatID, msg.replyToID, msg.text)
	default:
		return s.bot.SendMessage(ctx, msg.chatID, msg.text)
	}
}

func isReplyTargetGone(err error) bool {
	var apiErr *tg.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	desc := strings.ToLower(apiErr.Description)
	if strings.Contains(desc, "message to be replied") {
		return true
	}
	return strings.Contains(desc, "reply") &&
		(strings.Contains(desc, "not found") ||
			strings.Contains(desc, "invalid") ||
			strings.Contains(desc, "deleted"))
}

func (s *Store) notifyDead(msg message, sendErr error) {
	text := fmt.Sprintf("outbox dead id=%d: %s", msg.id, sendErr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = s.bot.SendMessage(ctx, msg.chatID, text)
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
create table if not exists outbox (
	id integer primary key autoincrement,
	chat_id integer not null,
	reply_to_id integer not null default 0,
	format text not null default '',
	text text not null,
	created_at integer not null,
	sent_at integer,
	dead_at integer,
	attempts integer not null default 0,
	last_error text not null default ''
);
create index if not exists outbox_pending on outbox(sent_at, dead_at, id);
`)
	if err != nil {
		return err
	}
	_, _ = s.db.Exec(`alter table outbox add column reply_to_id integer not null default 0`)
	_, _ = s.db.Exec(`alter table outbox add column format text not null default ''`)
	_, _ = s.db.Exec(`alter table outbox add column dead_at integer`)
	return err
}

func (s *Store) next() (message, error) {
	var msg message
	err := s.db.QueryRow(`
select id, chat_id, reply_to_id, format, text, attempts
from outbox
where sent_at is null and dead_at is null
order by id
limit 1
`).Scan(&msg.id, &msg.chatID, &msg.replyToID, &msg.format, &msg.text, &msg.attempts)
	return msg, err
}

func (s *Store) markSent(id int64) error {
	_, err := s.db.Exec(`update outbox set sent_at = unixepoch() where id = ?`, id)
	return err
}

func (s *Store) markDead(id int64) {
	_, _ = s.db.Exec(`update outbox set dead_at = unixepoch() where id = ?`, id)
}

func (s *Store) clearReplyTo(id int64) error {
	_, err := s.db.Exec(`update outbox set reply_to_id = 0 where id = ?`, id)
	return err
}

func (s *Store) recordError(id int64, sendErr error) {
	_, _ = s.db.Exec(`update outbox set last_error = ? where id = ?`, fmt.Sprint(sendErr), id)
}

func (s *Store) fail(id int64, attempts int, sendErr error) {
	_, _ = s.db.Exec(`update outbox set attempts = ?, last_error = ? where id = ?`,
		attempts, fmt.Sprint(sendErr), id)
}

func splitMessage(text string, limit int, format string) []string {
	if format == "html" {
		return splitHTMLMessage(text, limit)
	}
	if len(text) <= limit {
		return []string{text}
	}
	var chunks []string
	for len(text) > 0 {
		if len(text) <= limit {
			chunks = append(chunks, text)
			break
		}
		cut := limit
		if idx := strings.LastIndex(text[:limit], "\n"); idx > 0 {
			cut = idx
		}
		cut = alignUTF8Cut(text, cut)
		if cut <= 0 {
			_, size := utf8.DecodeRuneInString(text)
			cut = size
		}
		chunks = append(chunks, text[:cut])
		text = text[cut:]
		if len(text) > 0 && text[0] == '\n' {
			text = text[1:]
		}
	}
	return chunks
}

type htmlOpenTag struct {
	name string
	raw  string
}

func splitHTMLMessage(text string, limit int) []string {
	if len(text) <= limit {
		return []string{text}
	}

	var chunks []string
	var current strings.Builder
	var stack []htmlOpenTag
	i := 0
	for i < len(text) {
		if text[i] == '<' {
			end := strings.IndexByte(text[i:], '>')
			if end != -1 {
				token := text[i : i+end+1]
				name, closing, selfClosing, ok := parseHTMLTag(token)
				if ok {
					nextStack := stack
					if closing {
						if len(nextStack) > 0 && nextStack[len(nextStack)-1].name == name {
							nextStack = nextStack[:len(nextStack)-1]
						}
					} else if !selfClosing {
						nextStack = append(append([]htmlOpenTag(nil), stack...), htmlOpenTag{name: name, raw: token})
					}

					if current.Len() > 0 && current.Len()+len(token)+len(renderClosingTags(nextStack)) > limit {
						chunks = append(chunks, current.String()+renderClosingTags(stack))
						current.Reset()
						current.WriteString(renderOpeningTags(stack))
					}

					current.WriteString(token)
					stack = nextStack
					i += end + 1
					continue
				}
			}
		}

		nextTag := strings.IndexByte(text[i:], '<')
		end := len(text)
		if nextTag != -1 {
			end = i + nextTag
		}
		segment := text[i:end]
		for len(segment) > 0 {
			remaining := limit - current.Len() - len(renderClosingTags(stack))
			if remaining <= 0 && current.Len() > 0 {
				chunks = append(chunks, current.String()+renderClosingTags(stack))
				current.Reset()
				current.WriteString(renderOpeningTags(stack))
				continue
			}
			if len(segment) <= remaining {
				current.WriteString(segment)
				segment = ""
				continue
			}

			cut := htmlTextCut(segment, remaining)
			if cut <= 0 {
				if current.Len() > 0 {
					chunks = append(chunks, current.String()+renderClosingTags(stack))
					current.Reset()
					current.WriteString(renderOpeningTags(stack))
					continue
				}
				cut = remaining
			}

			current.WriteString(segment[:cut])
			segment = segment[cut:]
			chunks = append(chunks, current.String()+renderClosingTags(stack))
			current.Reset()
			current.WriteString(renderOpeningTags(stack))
		}
		i = end
	}

	if current.Len() > 0 {
		chunks = append(chunks, current.String()+renderClosingTags(stack))
	}
	return chunks
}

func parseHTMLTag(token string) (name string, closing bool, selfClosing bool, ok bool) {
	if len(token) < 3 || token[0] != '<' || token[len(token)-1] != '>' {
		return "", false, false, false
	}
	body := strings.TrimSpace(token[1 : len(token)-1])
	if body == "" {
		return "", false, false, false
	}
	if body[0] == '/' {
		closing = true
		body = strings.TrimSpace(body[1:])
	}
	if strings.HasSuffix(body, "/") {
		selfClosing = true
		body = strings.TrimSpace(strings.TrimSuffix(body, "/"))
	}
	if body == "" {
		return "", false, false, false
	}
	if idx := strings.IndexAny(body, " \t\r\n"); idx != -1 {
		body = body[:idx]
	}
	return strings.ToLower(body), closing, selfClosing, true
}

func renderOpeningTags(stack []htmlOpenTag) string {
	var sb strings.Builder
	for _, tag := range stack {
		sb.WriteString(tag.raw)
	}
	return sb.String()
}

func renderClosingTags(stack []htmlOpenTag) string {
	var sb strings.Builder
	for i := len(stack) - 1; i >= 0; i-- {
		sb.WriteString("</")
		sb.WriteString(stack[i].name)
		sb.WriteString(">")
	}
	return sb.String()
}

func htmlTextCut(text string, limit int) int {
	if len(text) <= limit {
		return len(text)
	}
	cut := limit
	if idx := strings.LastIndex(text[:limit], "\n"); idx > 0 {
		cut = idx
	} else if idx := strings.LastIndex(text[:limit], " "); idx > 0 {
		cut = idx
	}
	cut = avoidEntitySplit(text, cut)
	if cut <= 0 || cut > limit {
		cut = avoidEntitySplit(text, limit)
	}
	cut = alignUTF8Cut(text, cut)
	if cut <= 0 {
		_, size := utf8.DecodeRuneInString(text)
		return size
	}
	return cut
}

func avoidEntitySplit(text string, cut int) int {
	if cut <= 0 || cut >= len(text) {
		return cut
	}
	amp := strings.LastIndex(text[:cut], "&")
	if amp == -1 {
		return cut
	}
	if semi := strings.LastIndex(text[:cut], ";"); semi > amp {
		return cut
	}
	if end := strings.IndexByte(text[amp:], ';'); end != -1 && amp < cut {
		return amp
	}
	return cut
}

func alignUTF8Cut(text string, cut int) int {
	if cut <= 0 || cut >= len(text) {
		return cut
	}
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return cut
}
