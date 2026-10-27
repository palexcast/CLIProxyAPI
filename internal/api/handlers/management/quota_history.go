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
// header path, such as a Claude usage endpoint body relayed by APICall.
type quotaHistorySampleRecorder interface {
	RecordSample(auth *coreauth.Auth, sample quotahistory.Sample, now time.Time)
}

var _ quotaHistorySampleRecorder = (*quotahistory.Recorder)(nil)

type quotaHistoryAccount struct {
	AuthID   string `json:"auth_id"`
	Name     string `json:"name"`
	Label    string `json:"label"`
	Provider string `json:"provider"`
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
		Accounts: []quotaHistoryAccount{},
		Samples:  []quotahistory.Sample{},
	}
	for _, auth := range manager.List() {
		if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "claude") {
			continue
		}
		name := auth.ID
		if auth.FileName != "" {
			name = filepath.Base(auth.FileName)
		}
		resp.Accounts = append(resp.Accounts, quotaHistoryAccount{
			AuthID:   auth.ID,
			Name:     name,
			Label:    auth.Label,
			Provider: "claude",
		})
	}
	sort.Slice(resp.Accounts, func(i, j int) bool { return resp.Accounts[i].AuthID < resp.Accounts[j].AuthID })

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

// recordUsageQuotaHistory adds a quota-history sample for a successful Claude
// usage endpoint response relayed by APICall, so a quota refresh records the
// 5h/7d windows even when proxied responses omit the 7d headers.
func (h *Handler) recordUsageQuotaHistory(auth *coreauth.Auth, requestURL *url.URL, statusCode int, body []byte) {
	if h == nil || auth == nil || requestURL == nil {
		return
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "claude") ||
		!strings.EqualFold(requestURL.Hostname(), "api.anthropic.com") ||
		requestURL.Path != "/api/oauth/usage" {
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return
	}
	recorder, ok := manager.QuotaHistoryRecorder().(quotaHistorySampleRecorder)
	if !ok {
		return
	}
	sample, ok := quotahistory.ExtractFromUsage(body)
	if !ok {
		log.Debug("quota history: usage response carried no 5h/7d window")
		return
	}
	recorder.RecordSample(auth, sample, time.Now())
}
