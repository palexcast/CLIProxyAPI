package cliproxy

import (
	"context"
	"reflect"
	"testing"

	internalregistry "github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

const (
	extraModelTestID       = "claude-mythos-5-2"
	extraModelTestTemplate = "claude-fable-5-1"
)

func newClaudeExtraModelsAuth(id string, metadata map[string]any) *coreauth.Auth {
	return &coreauth.Auth{
		ID:         id,
		Provider:   "claude",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"auth_kind": "oauth"},
		Metadata:   metadata,
	}
}

func registerClaudeAuthsForTest(t *testing.T, service *Service, auths ...*coreauth.Auth) {
	t.Helper()
	registry := GlobalModelRegistry()
	for _, auth := range auths {
		registry.UnregisterClient(auth.ID)
		id := auth.ID
		t.Cleanup(func() { registry.UnregisterClient(id) })
		service.registerModelsForAuth(context.Background(), auth)
	}
}

func TestRegisterModelsForAuth_ClaudeExtraModelsOnlyOnListingCredential(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	seat := newClaudeExtraModelsAuth("auth-claude-extra-seat", map[string]any{
		"type": "claude",
		"extra_models": []any{
			map[string]any{"id": extraModelTestID, "like": extraModelTestTemplate, "display_name": "Claude Mythos 5.2"},
		},
	})
	plain := newClaudeExtraModelsAuth("auth-claude-extra-plain", map[string]any{"type": "claude"})
	registerClaudeAuthsForTest(t, service, seat, plain)

	registry := GlobalModelRegistry()
	if !registry.ClientSupportsModel(seat.ID, extraModelTestID) {
		t.Fatalf("credential listing %s must register it", extraModelTestID)
	}
	if registry.ClientSupportsModel(plain.ID, extraModelTestID) {
		t.Fatalf("credential without extra_models must not register %s", extraModelTestID)
	}
	if !registry.ClientSupportsModel(plain.ID, extraModelTestTemplate) || !registry.ClientSupportsModel(seat.ID, extraModelTestTemplate) {
		t.Fatalf("catalog model %s must stay registered on both credentials", extraModelTestTemplate)
	}

	info := internalregistry.LookupModelInfo(extraModelTestID, "claude")
	template := internalregistry.LookupModelInfo(extraModelTestTemplate, "claude")
	if info == nil || template == nil {
		t.Fatalf("lookup: extra=%v template=%v", info, template)
	}
	if info.DisplayName != "Claude Mythos 5.2" {
		t.Fatalf("display name = %q", info.DisplayName)
	}
	if template.Thinking == nil || !reflect.DeepEqual(info.Thinking, template.Thinking) {
		t.Fatalf("thinking = %+v, want template %+v", info.Thinking, template.Thinking)
	}
	if info.ContextLength != template.ContextLength || info.MaxCompletionTokens != template.MaxCompletionTokens {
		t.Fatalf("limits = %d/%d, want %d/%d", info.ContextLength, info.MaxCompletionTokens, template.ContextLength, template.MaxCompletionTokens)
	}
}

func TestRegisterModelsForAuth_ClaudeExtraModelsSkipsInvalidEntries(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	auth := newClaudeExtraModelsAuth("auth-claude-extra-invalid", map[string]any{
		"type": "claude",
		"extra_models": []any{
			"claude-mythos-5-2-string",
			map[string]any{"id": "claude-mythos-5-2-nolike"},
			map[string]any{"id": "claude-mythos-5-2-badlike", "like": "claude-not-in-catalog"},
			map[string]any{"id": extraModelTestID, "like": extraModelTestTemplate},
		},
	})
	registerClaudeAuthsForTest(t, service, auth)

	registry := GlobalModelRegistry()
	for _, skipped := range []string{"claude-mythos-5-2-string", "claude-mythos-5-2-nolike", "claude-mythos-5-2-badlike"} {
		if registry.ClientSupportsModel(auth.ID, skipped) {
			t.Fatalf("invalid entry %s must not register", skipped)
		}
	}
	if !registry.ClientSupportsModel(auth.ID, extraModelTestID) {
		t.Fatalf("valid entry %s must register despite invalid siblings", extraModelTestID)
	}
}

func TestRegisterModelsForAuth_ClaudeExtraModelsHonourExclusions(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	auth := newClaudeExtraModelsAuth("auth-claude-extra-excluded", map[string]any{
		"type":         "claude",
		"extra_models": []any{map[string]any{"id": extraModelTestID, "like": extraModelTestTemplate}},
	})
	auth.Attributes["excluded_models"] = "claude-mythos-*"
	registerClaudeAuthsForTest(t, service, auth)

	if GlobalModelRegistry().ClientSupportsModel(auth.ID, extraModelTestID) {
		t.Fatalf("excluded_models must still remove an extra model")
	}
}

func TestAppendAuthExtraModels_CatalogDefinitionWins(t *testing.T) {
	catalog := []*ModelInfo{{ID: extraModelTestTemplate, DisplayName: "Catalog", ContextLength: 7}}
	auth := newClaudeExtraModelsAuth("auth-claude-extra-dup", map[string]any{
		"extra_models": []any{map[string]any{"id": extraModelTestTemplate, "like": extraModelTestTemplate, "display_name": "Override"}},
	})
	out := appendAuthExtraModels(auth, catalog)
	if len(out) != 1 || out[0].DisplayName != "Catalog" {
		t.Fatalf("catalog entry must stay as is, got %+v", out)
	}
}
