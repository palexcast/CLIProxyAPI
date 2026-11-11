package management

import (
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const defaultQuotaHistoryRange = 14 * 24 * time.Hour

type quotaHistoryQuerier interface {
	Query(since, until time.Time, authIDs []string, step int64) ([]quotahistory.Sample, bool, error)
}

// quotaHistorySampleRecorder records a sample parsed outside the response
// header path, such as a Claude usage endpoint body relayed by APICall,
// without the per-account throttle.
type quotaHistorySampleRecorder interface {
	RecordSampleNow(auth *coreauth.Auth, sample quotahistory.Sample, now time.Time)
}

// quotaUsageRecorder keeps the last Claude usage endpoint body per account
// for the quota snapshot.
type quotaUsageRecorder interface {
	RecordUsage(auth *coreauth.Auth, body []byte, now time.Time)
}

// quotaProfileRecorder keeps the plan of a Claude OAuth profile body per
// account for the quota snapshot.
type quotaProfileRecorder interface {
	RecordProfile(auth *coreauth.Auth, body []byte, now time.Time)
}

// quotaSnapshotReader returns the latest known quota state keyed by auth id.
type quotaSnapshotReader interface {
	QuotaSnapshot() map[string]quotahistory.SnapshotEntry
}

var (
	_ quotaHistorySampleRecorder = (*quotahistory.Recorder)(nil)
	_ quotaUsageRecorder         = (*quotahistory.Recorder)(nil)
	_ quotaProfileRecorder       = (*quotahistory.Recorder)(nil)
	_ quotaSnapshotReader        = (*quotahistory.Recorder)(nil)
)

// quotaAccountIdentity names a Claude account in both quota endpoints.
type quotaAccountIdentity struct {
	AuthID   string `json:"auth_id"`
	Name     string `json:"name"`
	Label    string `json:"label"`
	Provider string `json:"provider"`
	// Disabled marks an account the operator switched off: it stays loaded but
	// takes no requests, so pooled capacity must leave it out.
	Disabled bool `json:"disabled"`
	// Priority is the credential's routing priority, read as the selector reads
	// it: new sessions and failover go to the highest available priority.
	Priority int `json:"priority"`
}

type quotaHistoryAccount struct {
	quotaAccountIdentity
	// Plan is the subscription from the account's last profile read through
	// /requests/api-call; absent until one is seen.
	Plan *quotahistory.Plan `json:"plan,omitempty"`
}

type quotaHistoryResponse struct {
	Since     string                `json:"since"`
	Until     string                `json:"until"`
	Step      int64                 `json:"step"`
	Accounts  []quotaHistoryAccount `json:"accounts"`
	Samples   []quotahistory.Sample `json:"samples"`
	Truncated bool                  `json:"truncated"`
}

// GetQuotaHistory returns the recorded Claude rate-limit window samples for a
// time range together with the Claude accounts currently loaded.
func (h *Handler) GetQuotaHistory(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}

	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	now := time.Now().UTC()
	until := now
	if raw := strings.TrimSpace(c.Query("until")); raw != "" {
		parsed, errParse := time.Parse(time.RFC3339, raw)
		if errParse != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid until: expected RFC3339 timestamp"})
			return
		}
		until = parsed.UTC()
	}
	since := now.Add(-defaultQuotaHistoryRange)
	if raw := strings.TrimSpace(c.Query("since")); raw != "" {
		parsed, errParse := time.Parse(time.RFC3339, raw)
		if errParse != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid since: expected RFC3339 timestamp"})
			return
		}
		since = parsed.UTC()
	}
	if since.After(until) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "since must not be after until"})
		return
	}
	var step int64
	if raw := strings.TrimSpace(c.Query("step")); raw != "" {
		parsed, errParse := strconv.ParseInt(raw, 10, 64)
		if errParse != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid step: expected non-negative integer seconds"})
			return
		}
		step = parsed
	}
	var authIDs []string
	for _, id := range c.QueryArray("auth_id") {
		if id = strings.TrimSpace(id); id != "" {
			authIDs = append(authIDs, id)
		}
	}

	resp := quotaHistoryResponse{
		Since:    since.Format(time.RFC3339),
		Until:    until.Format(time.RFC3339),
		Step:     step,
		Accounts: claudeQuotaAccounts(manager, quotaSnapshotEntries(manager)),
		Samples:  []quotahistory.Sample{},
	}

	if querier, ok := manager.QuotaHistoryRecorder().(quotaHistoryQuerier); ok {
		samples, truncated, errQuery := querier.Query(since, until, authIDs, step)
		if errQuery != nil {
			log.Errorf("failed to read quota history: %v", errQuery)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read quota history"})
			return
		}
		resp.Samples = samples
		resp.Truncated = truncated
	}

	c.JSON(http.StatusOK, resp)
}

// quotaSnapshotEntries returns the recorder's snapshot entries, or nil when the
// recorder keeps none.
func quotaSnapshotEntries(manager *coreauth.Manager) map[string]quotahistory.SnapshotEntry {
	if reader, ok := manager.QuotaHistoryRecorder().(quotaSnapshotReader); ok {
		return reader.QuotaSnapshot()
	}
	return nil
}

// claudeQuotaAccounts lists the Claude accounts the manager knows, sorted by
// auth id, each with the plan its snapshot entry holds.
func claudeQuotaAccounts(manager *coreauth.Manager, entries map[string]quotahistory.SnapshotEntry) []quotaHistoryAccount {
	accounts := []quotaHistoryAccount{}
	for _, auth := range manager.List() {
		if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "claude") {
			continue
		}
		name := auth.ID
		if auth.FileName != "" {
			name = filepath.Base(auth.FileName)
		}
		accounts = append(accounts, quotaHistoryAccount{
			quotaAccountIdentity: quotaAccountIdentity{
				AuthID:   auth.ID,
				Name:     name,
				Label:    auth.Label,
				Provider: "claude",
				Disabled: auth.Disabled,
				Priority: quotaAccountPriority(auth),
			},
			Plan: entries[auth.ID].Plan,
		})
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].AuthID < accounts[j].AuthID })
	return accounts
}

// quotaAccountPriority parses the auth's "priority" attribute like the
// selector does; 0 when absent or not an integer.
func quotaAccountPriority(auth *coreauth.Auth) int {
	if auth == nil || auth.Attributes == nil {
		return 0
	}
	priority, errAtoi := strconv.Atoi(strings.TrimSpace(auth.Attributes["priority"]))
	if errAtoi != nil {
		return 0
	}
	return priority
}

// quotaSnapshotAccount carries the plan through the snapshot entry.
type quotaSnapshotAccount struct {
	quotaAccountIdentity
	quotahistory.SnapshotEntry
}

type quotaSnapshotResponse struct {
	GeneratedAt string                 `json:"generated_at"`
	Accounts    []quotaSnapshotAccount `json:"accounts"`
}

// GetQuotaSnapshot returns the latest known quota state of every Claude
// account currently loaded, from proxied response headers and usage refreshes.
func (h *Handler) GetQuotaSnapshot(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}

	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	entries := quotaSnapshotEntries(manager)
	accounts := claudeQuotaAccounts(manager, entries)
	resp := quotaSnapshotResponse{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Accounts:    make([]quotaSnapshotAccount, 0, len(accounts)),
	}
	for _, account := range accounts {
		resp.Accounts = append(resp.Accounts, quotaSnapshotAccount{
			quotaAccountIdentity: account.quotaAccountIdentity,
			SnapshotEntry:        entries[account.AuthID],
		})
	}
	c.JSON(http.StatusOK, resp)
}

// recordClaudeAPICall feeds a successful Claude response relayed by APICall
// into the quota history. A usage endpoint response adds a sample, so a quota
// refresh records the 5h/7d windows even when proxied responses omit the 7d
// headers, and keeps the body in the quota snapshot; a profile response keeps
// the account's plan there.
func (h *Handler) recordClaudeAPICall(auth *coreauth.Auth, requestURL *url.URL, statusCode int, body []byte) {
	if h == nil || auth == nil || requestURL == nil {
		return
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "claude") ||
		!strings.EqualFold(requestURL.Hostname(), "api.anthropic.com") {
		return
	}
	if requestURL.Path != "/api/oauth/usage" && requestURL.Path != "/api/oauth/profile" {
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return
	}
	recorder := manager.QuotaHistoryRecorder()
	now := time.Now()
	if requestURL.Path == "/api/oauth/profile" {
		if profileRecorder, ok := recorder.(quotaProfileRecorder); ok {
			profileRecorder.RecordProfile(auth, body, now)
		}
		return
	}
	if usageRecorder, ok := recorder.(quotaUsageRecorder); ok {
		usageRecorder.RecordUsage(auth, body, now)
	}
	sample, ok := quotahistory.ExtractFromUsage(body)
	if !ok {
		log.Debug("quota history: usage response carried no 5h/7d window")
		return
	}
	if sampleRecorder, ok := recorder.(quotaHistorySampleRecorder); ok {
		sampleRecorder.RecordSampleNow(auth, sample, now)
	}
	liftClearedQuotaCooldown(manager, auth.ID, sample, now)
}
