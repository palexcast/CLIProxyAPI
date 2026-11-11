package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func liftFloat(v float64) *float64 { return &v }
func liftInt(v int64) *int64       { return &v }

// weeklyCooledAuth mirrors an account that a weekly 429 cooled down until
// deadline: a credential-wide quota block plus the model that took the 429.
func weeklyCooledAuth(id string, deadline time.Time) *coreauth.Auth {
	quota := coreauth.QuotaState{Exceeded: true, Reason: "credential_quota", NextRecoverAt: deadline}
	return &coreauth.Auth{
		ID:             id,
		Provider:       "claude",
		Status:         coreauth.StatusError,
		Unavailable:    true,
		NextRetryAfter: deadline,
		Quota:          quota,
		ModelStates: map[string]*coreauth.ModelState{
			"claude-opus-5-5": {
				Status:         coreauth.StatusError,
				Unavailable:    true,
				NextRetryAfter: deadline,
				Quota:          coreauth.QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: deadline},
			},
		},
	}
}

func TestUsageClearsQuotaCooldown(t *testing.T) {
	now := time.Date(2026, 11, 11, 7, 55, 57, 0, time.UTC)
	weeklyReset := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	// The 429 headers put the deadline a couple of seconds past the reset the
	// usage endpoint reports.
	deadline := weeklyReset.Add(2*time.Second + 37*time.Millisecond)
	fiveHourReset := now.Add(3 * time.Hour)

	tests := []struct {
		name   string
		auth   func() *coreauth.Auth
		sample quotahistory.Sample
		want   bool
	}{
		{
			name:   "weekly window freed before its reset",
			auth:   func() *coreauth.Auth { return weeklyCooledAuth("a", deadline) },
			sample: quotahistory.Sample{U5: liftFloat(0), U7: liftFloat(0.63), R7: liftInt(weeklyReset.Unix() - 1)},
			want:   true,
		},
		{
			name:   "weekly window still full",
			auth:   func() *coreauth.Auth { return weeklyCooledAuth("a", deadline) },
			sample: quotahistory.Sample{U7: liftFloat(1), R7: liftInt(weeklyReset.Unix()), S7: "rejected"},
		},
		{
			name:   "5h window full while the weekly has room",
			auth:   func() *coreauth.Auth { return weeklyCooledAuth("a", deadline) },
			sample: quotahistory.Sample{U5: liftFloat(1), R5: liftInt(fiveHourReset.Unix()), U7: liftFloat(0.63), R7: liftInt(weeklyReset.Unix())},
		},
		{
			name: "5h cooldown with the 5h window left out of the reading",
			auth: func() *coreauth.Auth {
				return weeklyCooledAuth("a", fiveHourReset)
			},
			sample: quotahistory.Sample{U7: liftFloat(0.42), R7: liftInt(weeklyReset.Unix())},
		},
		{
			name: "5h window freed before its reset",
			auth: func() *coreauth.Auth {
				return weeklyCooledAuth("a", fiveHourReset)
			},
			sample: quotahistory.Sample{U5: liftFloat(0.2), R5: liftInt(fiveHourReset.Unix()), U7: liftFloat(0.42), R7: liftInt(weeklyReset.Unix())},
			want:   true,
		},
		{
			name: "idle window without a reset explains no deadline",
			auth: func() *coreauth.Auth {
				return weeklyCooledAuth("a", fiveHourReset)
			},
			sample: quotahistory.Sample{U5: liftFloat(0), U7: liftFloat(0.42), R7: liftInt(weeklyReset.Unix())},
		},
		{
			name: "no cooldown",
			auth: func() *coreauth.Auth {
				return &coreauth.Auth{ID: "a", Provider: "claude", Status: coreauth.StatusActive}
			},
			sample: quotahistory.Sample{U7: liftFloat(0.63), R7: liftInt(weeklyReset.Unix())},
		},
		{
			name: "expired cooldown",
			auth: func() *coreauth.Auth { return weeklyCooledAuth("a", now.Add(-time.Minute)) },
			sample: quotahistory.Sample{
				U7: liftFloat(0.63), R7: liftInt(now.Add(-time.Minute).Unix()),
			},
		},
		{
			name: "a model blocked for a non-quota error",
			auth: func() *coreauth.Auth {
				a := weeklyCooledAuth("a", deadline)
				a.ModelStates["claude-haiku-5-5"] = &coreauth.ModelState{
					Status:         coreauth.StatusError,
					Unavailable:    true,
					NextRetryAfter: now.Add(10 * time.Minute),
				}
				return a
			},
			sample: quotahistory.Sample{U7: liftFloat(0.63), R7: liftInt(weeklyReset.Unix())},
		},
		{
			name: "a second model cooled to a deadline no window explains",
			auth: func() *coreauth.Auth {
				a := weeklyCooledAuth("a", deadline)
				other := weeklyReset.Add(24 * time.Hour)
				a.ModelStates["claude-fable-5-2"] = &coreauth.ModelState{
					Status:         coreauth.StatusError,
					Unavailable:    true,
					NextRetryAfter: other,
					Quota:          coreauth.QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: other},
				}
				return a
			},
			sample: quotahistory.Sample{U7: liftFloat(0.63), R7: liftInt(weeklyReset.Unix())},
		},
		{
			name: "disabled account",
			auth: func() *coreauth.Auth {
				a := weeklyCooledAuth("a", deadline)
				a.Disabled = true
				return a
			},
			sample: quotahistory.Sample{U7: liftFloat(0.63), R7: liftInt(weeklyReset.Unix())},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := usageClearsQuotaCooldown(tc.auth(), tc.sample, now); got != tc.want {
				t.Fatalf("usageClearsQuotaCooldown = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAPICallUsageLiftsClearedQuotaCooldown drives the panel's usage refresh
// through APICall and checks that a reading with room lifts the cooldown.
func TestAPICallUsageLiftsClearedQuotaCooldown(t *testing.T) {
	weeklyReset := time.Now().Add(72 * time.Hour).Truncate(time.Second).UTC()
	deadline := weeklyReset.Add(2 * time.Second)

	tests := []struct {
		name        string
		utilization string
		wantLifted  bool
	}{
		{name: "room", utilization: "63.0", wantLifted: true},
		{name: "full", utilization: "100.0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			usageBody := `{"five_hour":{"utilization":0,"resets_at":null},"seven_day":{"utilization":` + tc.utilization +
				`,"resets_at":"` + weeklyReset.Format(time.RFC3339) + `"}}`
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(usageBody))
			}))
			defer upstream.Close()

			manager := coreauth.NewManager(nil, nil, nil)
			auth := weeklyCooledAuth("claude-cooled", deadline)
			auth.Metadata = map[string]any{"access_token": "usage-test-token"}
			if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			h := &Handler{cfg: &config.Config{}, authManager: manager}
			router := gin.New()
			router.POST("/api/call", h.APICall)

			body := `{"auth_index":"` + auth.EnsureIndex() + `","method":"GET","url":"http://api.anthropic.com/api/oauth/usage"` +
				`,"proxy_url":"` + upstream.URL + `","header":{"Authorization":"Bearer $TOKEN$"}}`
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/call", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status code = %d; body = %s", rec.Code, rec.Body.String())
			}

			updated, ok := manager.GetByID("claude-cooled")
			if !ok {
				t.Fatal("auth missing after usage call")
			}
			lifted := !updated.Quota.Exceeded && !updated.Unavailable && updated.NextRetryAfter.IsZero()
			if lifted != tc.wantLifted {
				t.Fatalf("lifted = %v, want %v (quota %+v unavailable %v next %v)",
					lifted, tc.wantLifted, updated.Quota, updated.Unavailable, updated.NextRetryAfter)
			}
			if tc.wantLifted && updated.Status != coreauth.StatusActive {
				t.Fatalf("status = %q, want active", updated.Status)
			}
		})
	}
}
