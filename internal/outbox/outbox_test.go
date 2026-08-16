package outbox

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"picoman/internal/transport"
)

type fakeClient struct {
	chatID, replyTo, text, format string
	err                           error
	alwaysErr                     bool
	block                         <-chan struct{}
	sent                          chan struct{}
	failures                      atomic.Int32
	calls                         atomic.Int32
}

func (f *fakeClient) Send(ctx context.Context, chatID, replyTo, text, format string) (string, error) {
	f.calls.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	f.chatID, f.replyTo, f.text, f.format = chatID, replyTo, text, format
	if f.sent != nil {
		f.sent <- struct{}{}
	}
	if f.failures.Load() > 0 && f.failures.Add(-1) >= 0 {
		return "", f.err
	}
	if f.alwaysErr {
		return "", f.err
	}
	return "sent", nil
}

func TestFlushWaitsForDeferredRetry(t *testing.T) {
	client := &fakeClient{err: &transport.APIError{Platform: "max", Code: 503, Description: "temporary"}}
	client.failures.Store(1)
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"mx": client})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.EnqueueTo(transport.Address{Transport: "mx", ChatID: "2"}, "", "", "retry"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store.Flush(ctx)
	if got := client.calls.Load(); got < 2 {
		t.Fatalf("send calls = %d, want retry during flush", got)
	}
	if store.hasPending("mx") {
		t.Fatal("message remained pending after successful retry")
	}
}

func TestSlowTransportLaneDoesNotBlockAnother(t *testing.T) {
	blocked := make(chan struct{})
	mxClient := &fakeClient{block: blocked}
	tgClient := &fakeClient{sent: make(chan struct{}, 1)}
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"tg": tgClient, "mx": mxClient})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.EnqueueTo(transport.Address{Transport: "mx", ChatID: "2"}, "", "", "max"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueTo(transport.Address{Transport: "tg", ChatID: "1"}, "", "", "telegram"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { store.Run(ctx); close(done) }()
	select {
	case <-tgClient.sent:
	case <-time.After(time.Second):
		t.Fatal("Telegram was blocked by slow MAX delivery")
	}
	cancel()
	close(blocked)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("outbox did not stop")
	}
}

func TestDisabledTransportRowsWaitForReenable(t *testing.T) {
	client := &fakeClient{sent: make(chan struct{}, 1)}
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"mx": client})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var enabled atomic.Bool
	store.SetTransportEnabled(func(string) bool { return enabled.Load() })
	if err := store.EnqueueTo(transport.Address{Transport: "mx", ChatID: "2"}, "", "", "waiting"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { store.Run(ctx); close(done) }()
	select {
	case <-client.sent:
		t.Fatal("disabled transport delivered queued row")
	case <-time.After(50 * time.Millisecond):
	}
	enabled.Store(true)
	select {
	case <-client.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("queued row was not delivered after re-enable")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("outbox did not stop")
	}
}

func TestUnavailableTransportDoesNotBlockAnother(t *testing.T) {
	mxClient := &fakeClient{err: &transport.APIError{Platform: "max", Code: 503, Description: "down"}, alwaysErr: true}
	tgClient := &fakeClient{}
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"tg": tgClient, "mx": mxClient})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.EnqueueTo(transport.Address{Transport: "mx", ChatID: "2"}, "", "", "max"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueTo(transport.Address{Transport: "tg", ChatID: "1"}, "", "", "telegram"); err != nil {
		t.Fatal(err)
	}
	var retry *retryError
	if err := store.sendOne(context.Background(), "mx"); !errors.As(err, &retry) {
		t.Fatalf("first send error = %v, want retry", err)
	}
	if err := store.sendOne(context.Background(), "tg"); err != nil {
		t.Fatal(err)
	}
	if tgClient.text != "telegram" {
		t.Fatalf("Telegram message = %q", tgClient.text)
	}
}

func TestTransportLanePreservesFIFOAcrossDeferredRetry(t *testing.T) {
	client := &fakeClient{err: &transport.APIError{Platform: "max", Code: 503, Description: "temporary"}}
	client.failures.Store(1)
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"mx": client})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, text := range []string{"first", "second"} {
		if err := store.EnqueueTo(transport.Address{Transport: "mx", ChatID: "2"}, "", "", text); err != nil {
			t.Fatal(err)
		}
	}
	var retry *retryError
	if err := store.sendOne(context.Background(), "mx"); !errors.As(err, &retry) {
		t.Fatalf("first send error = %v", err)
	}
	if err := store.sendOne(context.Background(), "mx"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deferred head allowed overtaking: %v", err)
	}
	if calls := client.calls.Load(); calls != 1 {
		t.Fatalf("send calls = %d, want 1", calls)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := store.sendOne(context.Background(), "mx"); err != nil {
		t.Fatal(err)
	}
	if client.text != "first" {
		t.Fatalf("retried text = %q", client.text)
	}
	if err := store.sendOne(context.Background(), "mx"); err != nil {
		t.Fatal(err)
	}
	if client.text != "second" {
		t.Fatalf("next text = %q", client.text)
	}
}

func TestKeepsExistingTelegramSchemaAndRows(t *testing.T) {
	path := t.TempDir() + "/outbox.sqlite"
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`create table outbox (
		id integer primary key autoincrement, chat_id integer not null,
		reply_to_id integer not null default 0, format text not null default '', text text not null,
		created_at integer not null, sent_at integer, dead_at integer,
		attempts integer not null default 0, last_error text not null default '');
		insert into outbox(chat_id, reply_to_id, text, created_at) values(7, 8, 'pending', unixepoch());`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	tgClient := &fakeClient{}
	store, err := Open(path, map[string]transport.Client{"tg": tgClient})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SendOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tgClient.chatID != "7" || tgClient.replyTo != "8" || tgClient.text != "pending" {
		t.Fatalf("existing delivery = %#v", tgClient)
	}
	want := []string{"id", "chat_id", "reply_to_id", "format", "text", "created_at", "sent_at", "dead_at", "attempts", "last_error"}
	rows, err := store.db.Query(`select name from pragma_table_info('outbox') order by cid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		got = append(got, name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Telegram schema changed: %v", got)
	}
}

func (*fakeClient) Edit(context.Context, string, string, string, string) error { return nil }

func TestRoutesMessageToExactTransportAndReply(t *testing.T) {
	tgClient, mxClient := &fakeClient{}, &fakeClient{}
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"tg": tgClient, "mx": mxClient})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	address := transport.Address{Transport: "mx", ChatID: "123"}
	if err := store.EnqueueTo(address, "source", "html", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := store.SendOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if mxClient.chatID != "123" || mxClient.replyTo != "source" || mxClient.text != "hello" || mxClient.format != "html" {
		t.Fatalf("MAX delivery = %#v", mxClient)
	}
	if tgClient.text != "" {
		t.Fatal("message leaked to Telegram")
	}
}

func TestTelegramAndMAXUseSeparateTables(t *testing.T) {
	store, err := Open(t.TempDir()+"/outbox.sqlite", map[string]transport.Client{"tg": &fakeClient{}, "mx": &fakeClient{}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.EnqueueTo(transport.Address{Transport: "tg", ChatID: "123"}, "456", "html", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueTo(transport.Address{Transport: "mx", ChatID: "abc"}, "source", "", "max"); err != nil {
		t.Fatal(err)
	}
	var tgChat int64
	if err := store.db.QueryRow(`select chat_id from outbox where sent_at is null`).Scan(&tgChat); err != nil {
		t.Fatal(err)
	}
	var mxChat string
	if err := store.db.QueryRow(`select chat_id from max_outbox where sent_at is null`).Scan(&mxChat); err != nil {
		t.Fatal(err)
	}
	if tgChat != 123 || mxChat != "abc" {
		t.Fatalf("separate rows = tg:%d mx:%q", tgChat, mxChat)
	}
}
