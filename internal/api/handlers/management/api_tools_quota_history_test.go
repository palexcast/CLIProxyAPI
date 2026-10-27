package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const usageResponseBody = `{"five_hour":{"utilization":42.5,"resets_at":"2026-10-27T15:00:00.394837+00:00"},` +
	`"seven_day":{"utilization":16,"resets_at":"2026-10-31T09:00:00+00:00"},"extra_usage":{"is_enabled":false}}`

type fakeSampleRecorder struct {
	mu      sync.Mutex
	samples []quotahistory.Sample
	authIDs []string
}

func (f *fakeSampleRecorder) Record(*coreauth.Auth, http.Header, time.Time) {}

func (f *fakeSampleRecorder) RecordSample(auth *coreauth.Auth, sample quotahistory.Sample, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.samples = append(f.samples, sample)
	f.authIDs = append(f.authIDs, auth.ID)
}

func (f *fakeSampleRecorder) recorded() ([]quotahistory.Sample, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]quotahistory.Sample(nil), f.samples...), append([]string(nil), f.authIDs...)
}

func TestAPICallRecordsClaudeUsageQuotaHistory(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		url      string
		status   int
		want     bool
	}{
		{name: "claude usage", provider: "claude", url: "http://api.anthropic.com/api/oauth/usage?cedar_ember=1&skip_spend=1", status: http.StatusOK, want: true},
		{name: "other path", provider: "claude", url: "http://api.anthropic.com/api/oauth/profile", status: http.StatusOK},
		{name: "other host", provider: "claude", url: "http://example.com/api/oauth/usage", status: http.StatusOK},
		{name: "non-claude auth", provider: "codex", url: "http://api.anthropic.com/api/oauth/usage", status: http.StatusOK},
		{name: "upstream 4xx", provider: "claude", url: "http://api.anthropic.com/api/oauth/usage", status: http.StatusTooManyRequests},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authorization := make(chan string, 1)
			// The upstream acts as the request proxy, so the request URL keeps the
			// api.anthropic.com host the handler matches on.
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authorization <- r.Header.Get("Authorization")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(usageResponseBody))
			}))
			defer upstream.Close()

			manager := coreauth.NewManager(nil, nil, nil)
			auth := &coreauth.Auth{
				ID:       "claude-usage-a",
				Provider: tc.provider,
				Metadata: map[string]any{"access_token": "usage-test-token"},
			}
			if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			fake := &fakeSampleRecorder{}
			manager.SetQuotaHistoryRecorder(fake)
			h := &Handler{cfg: &config.Config{}, authManager: manager}
			router := gin.New()
			router.POST("/api/call", h.APICall)

			body := `{"auth_index":"` + auth.EnsureIndex() + `","method":"GET","url":"` + tc.url +
				`","proxy_url":"` + upstream.URL + `","header":{"Authorization":"Bearer $TOKEN$"}}`
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/call", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status code = %d; body = %s", rec.Code, rec.Body.String())
			}
			var response apiCallResponse
			if errDecode := json.NewDecoder(rec.Body).Decode(&response); errDecode != nil {
				t.Fatalf("decode response: %v", errDecode)
			}
			if response.StatusCode != tc.status || response.Body != usageResponseBody {
				t.Fatalf("relayed response = %d %q", response.StatusCode, response.Body)
			}
			if gotAuthorization := <-authorization; gotAuthorization != "Bearer usage-test-token" {
				t.Fatalf("upstream Authorization = %q", gotAuthorization)
			}

			samples, authIDs := fake.recorded()
			if !tc.want {
				if len(samples) != 0 {
					t.Fatalf("recorded samples = %+v, want none", samples)
				}
				return
			}
			if len(samples) != 1 || authIDs[0] != "claude-usage-a" {
				t.Fatalf("recorded samples = %+v for %v, want one for claude-usage-a", samples, authIDs)
			}
			s := samples[0]
			r5 := time.Date(2026, 10, 27, 15, 0, 0, 0, time.UTC).Unix()
			r7 := time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC).Unix()
			if s.U5 == nil || *s.U5 != 0.425 || s.R5 == nil || *s.R5 != r5 ||
				s.U7 == nil || *s.U7 != 0.16 || s.R7 == nil || *s.R7 != r7 || s.S5 != "" || s.S7 != "" {
				t.Fatalf("recorded sample = %+v", s)
			}
		})
	}
}

func TestAPICallUsageWithoutRecorder(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(usageResponseBody))
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{ID: "claude-usage-b", Provider: "claude", Metadata: map[string]any{"access_token": "t"}}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	h := &Handler{cfg: &config.Config{}, authManager: manager}
	router := gin.New()
	router.POST("/api/call", h.APICall)

	body := `{"auth_index":"` + auth.EnsureIndex() + `","method":"GET","url":"http://api.anthropic.com/api/oauth/usage","proxy_url":"` + upstream.URL + `"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/call", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d; body = %s", rec.Code, rec.Body.String())
	}
}
