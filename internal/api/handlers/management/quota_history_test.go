package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func serveQuotaHistory(t *testing.T, h *Handler, query string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(rec)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/observability/quota-history"+query, nil)
	h.GetQuotaHistory(ginCtx)
	return rec
}

func TestGetQuotaHistory(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	for _, auth := range []*coreauth.Auth{
		{ID: "claude-b", Provider: "claude", FileName: "/auths/claude-b-user@example.com.json", Label: "B team"},
		{ID: "claude-a", Provider: "claude", Label: "A"},
		{ID: "codex-a", Provider: "codex"},
	} {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	// Without a recorder the endpoint still answers with empty samples.
	rec := serveQuotaHistory(t, h, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var empty map[string]any
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &empty); errDecode != nil {
		t.Fatal(errDecode)
	}
	if samples, ok := empty["samples"].([]any); !ok || len(samples) != 0 || empty["truncated"] != false || empty["step"] != float64(0) {
		t.Fatalf("empty payload = %s", rec.Body.String())
	}

	store := quotahistory.NewStore(filepath.Join(t.TempDir(), quotahistory.DirName))
	u5, u7 := 0.42, 0.11
	r5, r7 := int64(1761580800), int64(1762000000)
	base := time.Date(2026, 10, 27, 11, 58, 3, 0, time.UTC)
	for _, s := range []quotahistory.Sample{
		{T: base, AuthID: "claude-b", U5: &u5, R5: &r5, U7: &u7, R7: &r7, S5: "allowed", S7: "allowed"},
		{T: base, AuthID: "claude-a", U5: &u5},
		{T: base.Add(-time.Hour), AuthID: "claude-gone", U7: &u7},
		{T: base.Add(-48 * time.Hour), AuthID: "claude-a", U5: &u5},
	} {
		if errAppend := store.Append(s); errAppend != nil {
			t.Fatal(errAppend)
		}
	}
	manager.SetQuotaHistoryRecorder(quotahistory.NewRecorder(store))

	rec = serveQuotaHistory(t, h, "?since=2026-10-27T00:00:00Z&until=2026-10-27T12:00:00%2B00:00")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload quotaHistoryResponse
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatal(errDecode)
	}
	if payload.Since != "2026-10-27T00:00:00Z" || payload.Until != "2026-10-27T12:00:00Z" || payload.Truncated || payload.Step != 0 {
		t.Fatalf("range = %s..%s truncated=%v step=%d", payload.Since, payload.Until, payload.Truncated, payload.Step)
	}
	wantAccounts := []quotaHistoryAccount{
		{AuthID: "claude-a", Name: "claude-a", Label: "A", Provider: "claude"},
		{AuthID: "claude-b", Name: "claude-b-user@example.com.json", Label: "B team", Provider: "claude"},
	}
	if len(payload.Accounts) != 2 || payload.Accounts[0] != wantAccounts[0] || payload.Accounts[1] != wantAccounts[1] {
		t.Fatalf("accounts = %#v", payload.Accounts)
	}
	if len(payload.Samples) != 3 || payload.Samples[0].AuthID != "claude-gone" ||
		payload.Samples[1].AuthID != "claude-a" || payload.Samples[2].AuthID != "claude-b" {
		t.Fatalf("samples = %#v", payload.Samples)
	}

	var raw struct {
		Samples []map[string]any `json:"samples"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &raw); errDecode != nil {
		t.Fatal(errDecode)
	}
	last := raw.Samples[2]
	if last["t"] != "2026-10-27T11:58:03Z" || last["u5"] != 0.42 || last["r5"] != float64(r5) ||
		last["u7"] != 0.11 || last["r7"] != float64(r7) || last["s5"] != "allowed" || last["s7"] != "allowed" {
		t.Fatalf("sample shape = %#v", last)
	}
	if _, present := raw.Samples[1]["u7"]; present {
		t.Fatalf("absent field serialized: %#v", raw.Samples[1])
	}

	rec = serveQuotaHistory(t, h, "?since=2026-10-25T00:00:00Z&until=2026-10-28T00:00:00Z&auth_id=claude-a&auth_id=claude-gone")
	payload = quotaHistoryResponse{}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatal(errDecode)
	}
	if len(payload.Samples) != 3 || payload.Samples[0].AuthID != "claude-a" || payload.Samples[1].AuthID != "claude-gone" {
		t.Fatalf("filtered samples = %#v", payload.Samples)
	}

	// step reduces each auth to its first sample, the last sample per bucket and
	// reset changes. claude-a's three samples on 2026-10-27 fall into one daily
	// bucket, so the middle one is dropped.
	more := base.Add(time.Minute)
	later := 0.5
	for _, at := range []time.Time{base.Add(30 * time.Second), more} {
		if errAppend := store.Append(quotahistory.Sample{T: at, AuthID: "claude-a", U5: &later}); errAppend != nil {
			t.Fatal(errAppend)
		}
	}
	rec = serveQuotaHistory(t, h, "?since=2026-10-27T00:00:00Z&until=2026-10-28T00:00:00Z&auth_id=claude-a&step=86400")
	payload = quotaHistoryResponse{}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatal(errDecode)
	}
	if rec.Code != http.StatusOK || payload.Step != 86400 || len(payload.Samples) != 2 ||
		!payload.Samples[0].T.Equal(base) || !payload.Samples[1].T.Equal(more) {
		t.Fatalf("step payload = %d %s", rec.Code, rec.Body.String())
	}
	rec = serveQuotaHistory(t, h, "?since=2026-10-27T00:00:00Z&until=2026-10-28T00:00:00Z&auth_id=claude-a&step=0")
	payload = quotaHistoryResponse{}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatal(errDecode)
	}
	if payload.Step != 0 || len(payload.Samples) != 3 {
		t.Fatalf("step=0 payload = %s", rec.Body.String())
	}

	for _, query := range []string{"?since=yesterday", "?until=2026-10-27", "?since=2026-10-28T00:00:00Z&until=2026-10-27T00:00:00Z", "?step=abc", "?step=-1", "?step=1.5"} {
		rec = serveQuotaHistory(t, h, query)
		var errBody map[string]string
		if rec.Code != http.StatusBadRequest || json.Unmarshal(rec.Body.Bytes(), &errBody) != nil || errBody["error"] == "" {
			t.Fatalf("%s: status = %d body=%s", query, rec.Code, rec.Body.String())
		}
	}
}

type fakeSnapshotReader struct {
	fakeSampleRecorder
	entries map[string]quotahistory.SnapshotEntry
}

func (f *fakeSnapshotReader) QuotaSnapshot() map[string]quotahistory.SnapshotEntry {
	return f.entries
}

func serveQuotaSnapshot(h *Handler) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(rec)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/observability/quota-snapshot", nil)
	h.GetQuotaSnapshot(ginCtx)
	return rec
}

func TestGetQuotaSnapshot(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	rec := serveQuotaSnapshot(NewHandlerWithoutConfigFilePath(&config.Config{}, nil))
	var errBody map[string]string
	if rec.Code != http.StatusServiceUnavailable || json.Unmarshal(rec.Body.Bytes(), &errBody) != nil || errBody["error"] == "" {
		t.Fatalf("no manager: status = %d body=%s", rec.Code, rec.Body.String())
	}

	manager := coreauth.NewManager(nil, nil, nil)
	for _, auth := range []*coreauth.Auth{
		{ID: "claude-b", Provider: "claude", FileName: "/auths/claude-b-user@example.com.json", Label: "B team"},
		{ID: "claude-a", Provider: "claude", Label: "A"},
		{ID: "claude-c", Provider: "claude"},
		{ID: "codex-a", Provider: "codex"},
	} {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	// Without a recorder every account is listed with nothing known.
	rec = serveQuotaSnapshot(h)
	if want := `[{"auth_id":"claude-a","name":"claude-a","label":"A","provider":"claude"},` +
		`{"auth_id":"claude-b","name":"claude-b-user@example.com.json","label":"B team","provider":"claude"},` +
		`{"auth_id":"claude-c","name":"claude-c","label":"","provider":"claude"}]`; rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"accounts":`+want) {
		t.Fatalf("no recorder: status = %d body=%s", rec.Code, rec.Body.String())
	}

	u5, u7 := 0.42, 0.11
	r5, r7 := int64(1761580800), int64(1762000000)
	observed := time.Date(2026, 10, 27, 11, 58, 3, 0, time.UTC)
	refreshed := time.Date(2026, 10, 27, 9, 15, 0, 0, time.UTC)
	manager.SetQuotaHistoryRecorder(&fakeSnapshotReader{entries: map[string]quotahistory.SnapshotEntry{
		"claude-b": {
			ObservedAt: &observed, U5: &u5, R5: &r5, S5: "allowed", U7: &u7, R7: &r7, S7: "allowed",
			RefreshedAt: &refreshed, Usage: json.RawMessage(`{"five_hour":{"utilization":42},"extra_usage":{"is_enabled":false}}`),
		},
		"claude-a":    {ObservedAt: &observed, U5: &u5},
		"claude-gone": {ObservedAt: &observed, U7: &u7},
	}})
	before := time.Now().UTC().Truncate(time.Second)
	rec = serveQuotaSnapshot(h)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		GeneratedAt string           `json:"generated_at"`
		Accounts    []map[string]any `json:"accounts"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatal(errDecode)
	}
	generated, errParse := time.Parse(time.RFC3339, payload.GeneratedAt)
	if errParse != nil || generated.Before(before) {
		t.Fatalf("generated_at = %q", payload.GeneratedAt)
	}
	if len(payload.Accounts) != 3 {
		t.Fatalf("accounts = %s", rec.Body.String())
	}
	a, b, c := payload.Accounts[0], payload.Accounts[1], payload.Accounts[2]
	if a["auth_id"] != "claude-a" || a["observed_at"] != "2026-10-27T11:58:03Z" || a["u5"] != 0.42 || len(a) != 6 {
		t.Fatalf("claude-a = %#v", a)
	}
	usage, _ := b["usage"].(map[string]any)
	if b["auth_id"] != "claude-b" || b["name"] != "claude-b-user@example.com.json" || b["label"] != "B team" ||
		b["u5"] != 0.42 || b["r5"] != float64(r5) || b["s5"] != "allowed" ||
		b["u7"] != 0.11 || b["r7"] != float64(r7) || b["s7"] != "allowed" ||
		b["refreshed_at"] != "2026-10-27T09:15:00Z" || usage == nil || usage["five_hour"] == nil || usage["extra_usage"] == nil {
		t.Fatalf("claude-b = %#v", b)
	}
	if c["auth_id"] != "claude-c" || len(c) != 4 {
		t.Fatalf("claude-c = %#v", c)
	}
}
