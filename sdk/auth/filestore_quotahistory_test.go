package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
)

// The quota snapshot lives under the auth directory, which List walks
// recursively; it must not register as a credential.
func TestFileTokenStoreListSkipsQuotaSnapshot(t *testing.T) {
	baseDir := t.TempDir()
	if errWrite := os.WriteFile(filepath.Join(baseDir, "claude-a.json"), []byte(`{"type":"claude","email":"a@example.com"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	now := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	historyDir := filepath.Join(baseDir, quotahistory.DirName)
	if errAppend := quotahistory.NewStore(historyDir).Append(quotahistory.Sample{T: now, AuthID: "claude-a.json", U5: new(0.1)}); errAppend != nil {
		t.Fatal(errAppend)
	}
	sn := quotahistory.NewSnapshot(historyDir)
	sn.ApplySample(quotahistory.Sample{T: now, AuthID: "claude-a.json", U5: new(0.1)})
	sn.ApplyUsage("claude-a.json", json.RawMessage(`{"five_hour":{"utilization":10}}`), now)
	if errFlush := sn.Flush(now, true); errFlush != nil {
		t.Fatal(errFlush)
	}
	if _, errStat := os.Stat(filepath.Join(historyDir, quotahistory.SnapshotFileName)); errStat != nil {
		t.Fatalf("snapshot not written: %v", errStat)
	}

	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auths, errList := store.List(context.Background())
	if errList != nil {
		t.Fatal(errList)
	}
	if len(auths) != 1 || auths[0].ID != "claude-a.json" {
		ids := make([]string, 0, len(auths))
		for _, a := range auths {
			ids = append(ids, a.ID+"("+a.Provider+")")
		}
		t.Fatalf("listed auths = %v, want only claude-a.json", ids)
	}
}
