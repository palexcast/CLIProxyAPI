package synthesizer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func synthesizeSingleAuthFile(t *testing.T, authData map[string]any) string {
	t.Helper()
	tempDir := t.TempDir()
	data, errMarshal := json.Marshal(authData)
	if errMarshal != nil {
		t.Fatalf("marshal auth data: %v", errMarshal)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "claude-auth.json"), data, 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		IDGenerator: NewStableIDGenerator(),
	}
	auths, err := NewFileSynthesizer().Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 1 {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}
	return auths[0].Label
}

func TestFileSynthesizerPrefersExplicitLabel(t *testing.T) {
	label := synthesizeSingleAuthFile(t, map[string]any{
		"type":  "claude",
		"email": "test@example.com",
		"label": "  Team account  ",
	})
	if label != "Team account" {
		t.Fatalf("expected explicit label to win, got %q", label)
	}
}

func TestFileSynthesizerFallsBackToEmailWhenLabelBlank(t *testing.T) {
	label := synthesizeSingleAuthFile(t, map[string]any{
		"type":  "claude",
		"email": "test@example.com",
		"label": "   ",
	})
	if label != "test@example.com" {
		t.Fatalf("expected email fallback, got %q", label)
	}
}
