package management

import (
	"context"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/quotahistory"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// quotaCooldownResetTolerance is how far a usage window's reset may sit from a
// cooldown deadline and still be the window that set it. A 429's deadline is
// the rejected window's reset as sent in the headers, a second or two off the
// reset the usage endpoint reports for the same window.
const quotaCooldownResetTolerance = 2 * time.Minute

// liftClearedQuotaCooldown clears a Claude account's quota cooldown when a
// fresh usage endpoint reading shows the window that set it has room again.
//
// A 429 cools an account down until the rejected window resets, which for the
// weekly window can be days away. Anthropic can free a window before then (a
// raised seat limit, usage credited back), and nothing else lifts the
// cooldown early, so the pool keeps skipping an account that would serve.
func liftClearedQuotaCooldown(manager *coreauth.Manager, authID string, sample quotahistory.Sample, now time.Time) {
	if manager == nil {
		return
	}
	current, ok := manager.GetByID(authID)
	if !ok || !usageClearsQuotaCooldown(current, sample, now) {
		return
	}
	if _, _, errReset := manager.ResetQuota(context.Background(), authID); errReset != nil {
		log.WithError(errReset).WithField("auth_index", current.EnsureIndex()).Warn("quota cooldown: failed to lift after usage reading showed room")
		return
	}
	log.WithFields(log.Fields{
		"auth_index": current.EnsureIndex(),
		"label":      current.Label,
	}).Info("quota cooldown: lifted, usage reading shows the limiting window has room")
}

// usageClearsQuotaCooldown reports whether sample shows that every live quota
// cooldown on auth has ended early. It holds only when:
//   - the auth has at least one quota cooldown that has not expired yet;
//   - no non-quota block is live (the reset would clear that as well);
//   - every 5h or 7d window the reading reports is below 100%;
//   - each cooldown deadline matches the reset of a window the reading reports.
//
// The usage endpoint sometimes leaves a window out (a 5h window right after a
// 5h rejection), so a deadline no reported window explains keeps the cooldown.
func usageClearsQuotaCooldown(auth *coreauth.Auth, sample quotahistory.Sample, now time.Time) bool {
	if auth == nil || auth.Disabled {
		return false
	}
	if !windowHasRoom(sample.U5) || !windowHasRoom(sample.U7) {
		return false
	}
	var deadlines []time.Time
	collect := func(unavailable bool, nextRetry time.Time, quota coreauth.QuotaState) bool {
		if quota.Exceeded && quota.NextRecoverAt.After(now) {
			deadlines = append(deadlines, quota.NextRecoverAt)
			return true
		}
		// A live block that is not a quota cooldown: leave the auth alone.
		return !(unavailable && nextRetry.After(now))
	}
	if !collect(auth.Unavailable, auth.NextRetryAfter, auth.Quota) {
		return false
	}
	for _, state := range auth.ModelStates {
		if state == nil {
			continue
		}
		if !collect(state.Unavailable, state.NextRetryAfter, state.Quota) {
			return false
		}
	}
	if len(deadlines) == 0 {
		return false
	}
	for _, deadline := range deadlines {
		if !resetMatches(sample.U5, sample.R5, deadline) && !resetMatches(sample.U7, sample.R7, deadline) {
			return false
		}
	}
	return true
}

// windowHasRoom reports whether a window is absent from the reading or below
// 100%.
func windowHasRoom(utilization *float64) bool {
	return utilization == nil || *utilization < 1
}

// resetMatches reports whether a reported window with room resets at deadline.
func resetMatches(utilization *float64, reset *int64, deadline time.Time) bool {
	if utilization == nil || reset == nil || *utilization >= 1 {
		return false
	}
	diff := time.Unix(*reset, 0).Sub(deadline)
	if diff < 0 {
		diff = -diff
	}
	return diff <= quotaCooldownResetTolerance
}
