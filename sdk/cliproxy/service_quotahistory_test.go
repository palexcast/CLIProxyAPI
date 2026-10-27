package cliproxy

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestQuotaHistoryRecordsAfterRunContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		authDir := t.TempDir()
		s := &Service{coreManager: coreauth.NewManager(nil, nil, nil)}
		runCtx, runCancel := context.WithCancel(context.Background())
		s.startQuotaHistory(runCtx, &config.Config{AuthDir: authDir})
		recorder := s.coreManager.QuotaHistoryRecorder()
		if recorder == nil {
			t.Fatal("quota history recorder was not installed")
		}

		// Shutdown cancels the run context first and stops the API server
		// later; a response completing in between must still be recorded.
		runCancel()
		synctest.Wait()
		s.cfgMu.RLock()
		done := s.quotaHistoryDone
		s.cfgMu.RUnlock()
		select {
		case <-done:
			t.Fatal("quota history writer stopped with the run context")
		default:
		}
		now := time.Now()
		recorder.Record(&coreauth.Auth{ID: "claude-a", Provider: "claude"}, http.Header{
			"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.3"},
		}, now)

		s.stopQuotaHistory()
		<-done
		samples, _, errQuery := quotahistory.NewStore(filepath.Join(authDir, quotahistory.DirName)).
			Query(now.Add(-time.Hour), now.Add(time.Hour), nil, 0, 0)
		if errQuery != nil {
			t.Fatal(errQuery)
		}
		if len(samples) != 1 || samples[0].AuthID != "claude-a" {
			t.Fatalf("samples = %+v", samples)
		}
	})
}

func TestStopQuotaHistoryWithoutRecorder(t *testing.T) {
	s := &Service{}
	s.stopQuotaHistory()
	s.stopQuotaHistory()
}
