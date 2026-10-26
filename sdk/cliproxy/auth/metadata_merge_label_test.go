package auth

import "testing"

func TestMergeAuthContentReplacesLabelFromReload(t *testing.T) {
	base := &Auth{ID: "claude-a.json", Label: "user@example.com", Metadata: map[string]any{"email": "user@example.com"}}
	current := base.Clone()
	updated := &Auth{ID: "claude-a.json", Label: "Team account", Metadata: map[string]any{"email": "user@example.com", "label": "Team account"}}

	merged := mergeAuthContent(base, current, updated)
	if merged == nil {
		t.Fatal("expected merged auth")
	}
	if merged.Label != "Team account" {
		t.Fatalf("expected label to follow the reloaded file, got %q", merged.Label)
	}
	if got, _ := merged.Metadata["label"].(string); got != "Team account" {
		t.Fatalf("expected label metadata to be merged, got %q", got)
	}
}

func TestMergeAuthContentKeepsLabelWhenUpdateHasNone(t *testing.T) {
	base := &Auth{ID: "claude-a.json", Label: "Team account", Metadata: map[string]any{"label": "Team account"}}
	current := base.Clone()
	updated := &Auth{ID: "claude-a.json", Metadata: map[string]any{"label": "Team account"}}

	merged := mergeAuthContent(base, current, updated)
	if merged.Label != "Team account" {
		t.Fatalf("expected current label to be kept, got %q", merged.Label)
	}
}
