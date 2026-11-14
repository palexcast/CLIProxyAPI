package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// A model only some credentials register (a per-credential extra model) must
// route to those credentials alone: a session bound elsewhere for another
// model moves to them, a higher-priority credential without the model never
// takes it, and when every credential with the model cools down the request
// fails instead of falling back to one without it.
func TestManagerRoutesCredentialOnlyModelToListingCredentials(t *testing.T) {
	withQuotaCooldownEnabled(t)
	ctx := context.Background()
	const shared, exclusive = "claude-fable-5-1", "claude-mythos-5-2"
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&mockCustomErrorExecutor{identifier: "claude"})

	plainID, seatID := "plain-"+t.Name(), "seat-"+t.Name()
	for _, candidate := range []struct {
		auth   *Auth
		models []*registry.ModelInfo
	}{
		{&Auth{ID: plainID, Provider: "claude", Status: StatusActive, Attributes: map[string]string{"priority": "11"}}, []*registry.ModelInfo{{ID: shared}}},
		{&Auth{ID: seatID, Provider: "claude", Status: StatusActive, Attributes: map[string]string{"priority": "10"}}, []*registry.ModelInfo{{ID: shared}, {ID: exclusive}}},
	} {
		registry.GetGlobalRegistry().RegisterClient(candidate.auth.ID, "claude", candidate.models)
		id := candidate.auth.ID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		if _, errRegister := manager.Register(ctx, candidate.auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Session-Id": []string{"session-" + t.Name()}}}
	pick := func(model string) (string, error) {
		auth, errSelect := manager.SelectAuth(ctx, "claude", model, opts)
		if errSelect != nil {
			return "", errSelect
		}
		return auth.ID, nil
	}

	for i := 0; i < 3; i++ {
		if got, errPick := pick(shared); errPick != nil || got != plainID {
			t.Fatalf("round %d shared model = %q, %v; want the higher priority %s", i, got, errPick, plainID)
		}
		if got, errPick := pick(exclusive); errPick != nil || got != seatID {
			t.Fatalf("round %d exclusive model = %q, %v; want %s", i, got, errPick, seatID)
		}
	}

	retryAfter := time.Hour
	manager.MarkResult(ctx, Result{
		AuthID: seatID, Provider: "claude", Model: exclusive, RetryAfter: &retryAfter,
		Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota"},
	})
	if got, errPick := pick(exclusive); errPick == nil {
		t.Fatalf("exclusive model with its only credential cooling down went to %q; want an error", got)
	}
}
