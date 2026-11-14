package cliproxy

import (
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// authExtraModelsKey is the credential metadata key listing models this one
// credential serves beyond the provider catalog, for models an account plan
// unlocks that the shared catalog does not list. Each entry is an object:
//
//	{"id": "claude-mythos-5-2", "like": "claude-fable-5-2", "display_name": "Claude Mythos 5.2"}
//
// "id" is required. "like" names a catalog model whose definition (context
// length, thinking levels, capabilities) the extra model copies; it is
// required so a model never registers without the thinking metadata the
// request pipeline reads. "display_name" is optional and defaults to the id.
// Only this credential registers the model, so routing sends requests for it
// to the credentials that list it and to no others.
const authExtraModelsKey = "extra_models"

type authExtraModel struct {
	id          string
	like        string
	displayName string
}

// authExtraModelsFromMetadata parses the extra_models entries of a credential.
// Malformed entries are skipped with a warning so one bad entry never drops
// the credential or its other models.
func authExtraModelsFromMetadata(auth *coreauth.Auth) []authExtraModel {
	if auth == nil || auth.Metadata == nil {
		return nil
	}
	raw, ok := auth.Metadata[authExtraModelsKey]
	if !ok || raw == nil {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		log.Warnf("auth %s: %s must be an array of objects, got %T", auth.ID, authExtraModelsKey, raw)
		return nil
	}
	out := make([]authExtraModel, 0, len(items))
	for index, item := range items {
		entry, okEntry := item.(map[string]any)
		if !okEntry {
			log.Warnf("auth %s: %s[%d] must be an object, got %T", auth.ID, authExtraModelsKey, index, item)
			continue
		}
		id, _ := entry["id"].(string)
		like, _ := entry["like"].(string)
		displayName, _ := entry["display_name"].(string)
		id, like, displayName = strings.TrimSpace(id), strings.TrimSpace(like), strings.TrimSpace(displayName)
		if id == "" || like == "" {
			log.Warnf("auth %s: %s[%d] needs both id and like", auth.ID, authExtraModelsKey, index)
			continue
		}
		out = append(out, authExtraModel{id: id, like: like, displayName: displayName})
	}
	return out
}

// appendAuthExtraModels adds the credential's extra models to its catalog
// models. Each copies the catalog definition named by "like"; an entry whose
// template is missing from the catalog is skipped with a warning and picked
// up when a catalog refresh re-registers the credential. An id the catalog
// already lists keeps the catalog definition.
func appendAuthExtraModels(auth *coreauth.Auth, models []*ModelInfo) []*ModelInfo {
	extras := authExtraModelsFromMetadata(auth)
	if len(extras) == 0 {
		return models
	}
	byID := make(map[string]*ModelInfo, len(models))
	for _, model := range models {
		if model != nil {
			byID[strings.ToLower(strings.TrimSpace(model.ID))] = model
		}
	}
	for _, extra := range extras {
		key := strings.ToLower(extra.id)
		if _, exists := byID[key]; exists {
			continue
		}
		template, okTemplate := byID[strings.ToLower(extra.like)]
		if !okTemplate {
			log.Warnf("auth %s: extra model %s skipped, template %s is not in the catalog", auth.ID, extra.id, extra.like)
			continue
		}
		clone := cloneModelInfoForCatalogRoute(template)
		clone.ID = extra.id
		clone.Name = ""
		clone.DisplayName = extra.displayName
		if clone.DisplayName == "" {
			clone.DisplayName = extra.id
		}
		clone.Description = ""
		models = append(models, &clone)
		byID[key] = &clone
	}
	return models
}
