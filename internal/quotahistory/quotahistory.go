// Package quotahistory records the Claude unified rate-limit headers observed on
// upstream responses as a per-day JSON Lines history, so the management API can
// show how the 5-hour and 7-day windows were used over time.
package quotahistory

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	// DirName is the history directory created inside the auth directory.
	DirName = "quota-history"
	// MaxQuerySamples caps a single query; the oldest samples are dropped first.
	MaxQuerySamples = 200000

	throttleInterval = 60 * time.Second
	// retentionDays covers an 8-week range plus the 7 days before it, where a
	// weekly window still open at the range start began.
	retentionDays    = 63
	pruneInterval    = 24 * time.Hour
	queueSize        = 1024
	dropWarnInterval = time.Minute
	maxStatusLen     = 64
	maxLineBytes     = 1 << 20
	dayLayout        = "2006-01-02"
	fileExt          = ".jsonl"
)

// Sample is one observation of an account's rate-limit windows. It is both the
// on-disk line format and the API response shape.
type Sample struct {
	T      time.Time `json:"t"`
	AuthID string    `json:"auth_id"`
	U5     *float64  `json:"u5,omitempty"`
	R5     *int64    `json:"r5,omitempty"`
	U7     *float64  `json:"u7,omitempty"`
	R7     *int64    `json:"r7,omitempty"`
	S5     string    `json:"s5,omitempty"`
	S7     string    `json:"s7,omitempty"`
}

// Extract builds a sample from Claude response headers. It reports false when
// neither utilization header is present with a value in [0, 1].
func Extract(headers http.Header) (Sample, bool) {
	var s Sample
	if len(headers) == 0 {
		return s, false
	}
	s.U5 = parseUtilization(header(headers, "Anthropic-Ratelimit-Unified-5h-Utilization"))
	s.U7 = parseUtilization(header(headers, "Anthropic-Ratelimit-Unified-7d-Utilization"))
	if s.U5 == nil && s.U7 == nil {
		return Sample{}, false
	}
	s.R5 = parseReset(header(headers, "Anthropic-Ratelimit-Unified-5h-Reset"))
	s.R7 = parseReset(header(headers, "Anthropic-Ratelimit-Unified-7d-Reset"))
	s.S5 = parseStatus(header(headers, "Anthropic-Ratelimit-Unified-5h-Status"))
	s.S7 = parseStatus(header(headers, "Anthropic-Ratelimit-Unified-7d-Status"))
	return s, true
}

// usageWindow is one plan window of the Claude OAuth usage endpoint response.
type usageWindow struct {
	Utilization json.RawMessage `json:"utilization"`
	ResetsAt    *string         `json:"resets_at"`
}

// ExtractFromUsage builds a sample from the JSON body of the Claude OAuth usage
// endpoint (/api/oauth/usage). That endpoint reports utilization as a
// percentage in [0, 100], which is stored as a fraction like the headers. A
// window without a valid utilization is omitted; it reports false when neither
// window parsed. The endpoint carries no status, so s5/s7 are only set to
// "rejected" when a window is fully used.
func ExtractFromUsage(body []byte) (Sample, bool) {
	var payload struct {
		FiveHour *usageWindow `json:"five_hour"`
		SevenDay *usageWindow `json:"seven_day"`
	}
	if errDecode := json.Unmarshal(body, &payload); errDecode != nil {
		return Sample{}, false
	}
	var s Sample
	s.U5, s.R5, s.S5 = parseUsageWindow(payload.FiveHour)
	s.U7, s.R7, s.S7 = parseUsageWindow(payload.SevenDay)
	if s.U5 == nil && s.U7 == nil {
		return Sample{}, false
	}
	return s, true
}

func parseUsageWindow(w *usageWindow) (*float64, *int64, string) {
	if w == nil {
		return nil, nil, ""
	}
	raw := strings.Trim(strings.TrimSpace(string(w.Utilization)), `"`)
	percent, errParse := strconv.ParseFloat(raw, 64)
	if errParse != nil || math.IsNaN(percent) || percent < 0 || percent > 100 {
		return nil, nil, ""
	}
	fraction := percent / 100
	status := ""
	if fraction >= 1 {
		status = "rejected"
	}
	// A window without a valid reset is not recorded: a reading with no window
	// cannot be placed, and the panel already treats a passed reset as refilled.
	if w.ResetsAt == nil {
		return nil, nil, ""
	}
	t, errTime := time.Parse(time.RFC3339, strings.TrimSpace(*w.ResetsAt))
	if errTime != nil {
		return nil, nil, ""
	}
	reset := t.Unix()
	return &fraction, &reset, status
}

// windowChanged reports whether s starts a new window relative to prev. A reset
// moving by more than resetTolerance is a change, and so is utilization going
// down, which never happens inside one window and does not depend on the reset
// headers being present.
func windowChanged(prev, s Sample) bool {
	return changed5(prev, s) || changed7(prev, s)
}

// changed5 and changed7 judge one window at a time, so a sample that carries only
// the other window never counts as a change.
func changed5(prev, s Sample) bool {
	return !sameReset(prev.R5, s.R5) || utilizationDropped(prev.U5, s.U5)
}

func changed7(prev, s Sample) bool {
	return !sameReset(prev.R7, s.R7) || utilizationDropped(prev.U7, s.U7)
}

// carries5 and carries7 report whether a sample says anything about that window.
func carries5(s Sample) bool { return s.U5 != nil || s.R5 != nil }
func carries7(s Sample) bool { return s.U7 != nil || s.R7 != nil }

// utilizationTolerance is how far utilization may go down and still describe the
// same window. Headers carry a fraction with several decimals while the usage
// endpoint carries a one-decimal percentage, so 0.1234 followed by 12.3% is the
// same reading at a lower precision, not a new window.
const utilizationTolerance = 0.001

func utilizationDropped(prev, cur *float64) bool {
	return prev != nil && cur != nil && *prev-*cur >= utilizationTolerance
}

func header(h http.Header, name string) string {
	if v := h.Get(name); v != "" {
		return v
	}
	for k, v := range h {
		if strings.EqualFold(k, name) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func parseUtilization(raw string) *float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(v) || v < 0 || v > 1 {
		return nil
	}
	return &v
}

func parseReset(raw string) *int64 {
	t, ok := helps.ParseUnixOrTimestamp(raw)
	if !ok {
		return nil
	}
	v := t.Unix()
	return &v
}

func parseStatus(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if len(v) > maxStatusLen {
		return ""
	}
	return v
}

// resetTolerance is how far two reset times may differ and still describe the same
// window. Response headers and the usage endpoint can report the same reset a
// second apart, and a header that is present on one response and absent on the
// next says nothing about the window.
const resetTolerance = 60

// sameReset reports whether two reset readings describe the same window. A
// missing reading is unknown rather than different, so only two present readings
// more than resetTolerance seconds apart count as a window change.
func sameReset(a, b *int64) bool {
	if a == nil || b == nil {
		return true
	}
	d := *a - *b
	if d < 0 {
		d = -d
	}
	return d <= resetTolerance
}

// Store reads and writes day files under one directory.
type Store struct {
	dir string
}

// NewStore returns a store rooted at dir. The directory is created on first write.
func NewStore(dir string) *Store {
	return &Store{dir: dir}
}

func (st *Store) dayPath(day time.Time) string {
	return filepath.Join(st.dir, day.UTC().Format(dayLayout)+fileExt)
}

// Append writes one sample to the day file of its timestamp.
func (st *Store) Append(s Sample) error {
	if errMkdir := os.MkdirAll(st.dir, 0o700); errMkdir != nil {
		return fmt.Errorf("create quota history dir: %w", errMkdir)
	}
	line, errMarshal := json.Marshal(s)
	if errMarshal != nil {
		return fmt.Errorf("encode quota history sample: %w", errMarshal)
	}
	f, errOpen := os.OpenFile(st.dayPath(s.T), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if errOpen != nil {
		return fmt.Errorf("open quota history file: %w", errOpen)
	}
	_, errWrite := f.Write(append(line, '\n'))
	if errClose := f.Close(); errWrite == nil && errClose != nil {
		errWrite = errClose
	}
	if errWrite != nil {
		return fmt.Errorf("write quota history file: %w", errWrite)
	}
	return nil
}

// days lists the dates of the day files present in the store, ascending.
func (st *Store) days() ([]time.Time, error) {
	entries, errRead := os.ReadDir(st.dir)
	if errRead != nil {
		if os.IsNotExist(errRead) {
			return nil, nil
		}
		return nil, fmt.Errorf("list quota history dir: %w", errRead)
	}
	var out []time.Time
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, fileExt) {
			continue
		}
		day, errParse := time.Parse(dayLayout, strings.TrimSuffix(name, fileExt))
		if errParse != nil {
			continue
		}
		out = append(out, day)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out, nil
}

// Query returns samples with since <= t <= until, restricted to authIDs when
// non-empty, sorted by time then auth id. A positive step (seconds) reduces each
// auth's samples to its first one in the range, the last one per step-aligned
// bucket and both samples around every change of a window's reset time; step 0
// returns raw samples.
// When more than limit samples remain, the oldest are dropped and truncated is
// true. Day files are processed in date order and the result is trimmed after
// each one, so memory stays bounded by the output plus one day file.
func (st *Store) Query(since, until time.Time, authIDs []string, step int64, limit int) (samples []Sample, truncated bool, err error) {
	days, errDays := st.days()
	if errDays != nil {
		return nil, false, errDays
	}
	var allow map[string]struct{}
	if len(authIDs) > 0 {
		allow = make(map[string]struct{}, len(authIDs))
		for _, id := range authIDs {
			allow[id] = struct{}{}
		}
	}
	trim := func() {
		if limit <= 0 || len(samples) <= limit {
			return
		}
		sortSamples(samples)
		samples = append(samples[:0], samples[len(samples)-limit:]...)
		truncated = true
	}
	reducer := newStepReducer(step)
	firstDay := since.UTC().Truncate(24 * time.Hour)
	samples = []Sample{}
	var dayBuf []Sample
	for _, day := range days {
		if day.Before(firstDay) || day.After(until) {
			continue
		}
		dayBuf = dayBuf[:0]
		if errRead := st.readDay(day, func(s Sample) {
			if s.T.Before(since) || s.T.After(until) {
				return
			}
			if allow != nil {
				if _, ok := allow[s.AuthID]; !ok {
					return
				}
			}
			dayBuf = append(dayBuf, s)
		}); errRead != nil {
			return nil, false, errRead
		}
		// A day file holds only samples of that UTC day, so sorting each file
		// keeps every auth's samples in time order across files.
		sortSamples(dayBuf)
		if reducer == nil {
			samples = append(samples, dayBuf...)
		} else {
			for _, s := range dayBuf {
				samples = reducer.add(samples, s)
			}
		}
		trim()
	}
	if reducer != nil {
		samples = reducer.flush(samples)
	}
	sortSamples(samples)
	trim()
	return samples, truncated, nil
}

func sortSamples(samples []Sample) {
	sort.SliceStable(samples, func(i, j int) bool {
		if !samples[i].T.Equal(samples[j].T) {
			return samples[i].T.Before(samples[j].T)
		}
		return samples[i].AuthID < samples[j].AuthID
	})
}

// stepReducer keeps, per auth, the first sample in the range and, for each
// window (5h, 7d) on its own, the last sample carrying that window in every step
// bucket plus both neighbouring samples around a change of that window. Judging
// the windows separately keeps a sparse 7d reading (from a usage refresh) among
// dense 5h-only header samples, and finds a 7d change across samples that do not
// carry 7d at all. Samples must be added in time order per auth.
type stepReducer struct {
	step int64
	last map[string]*stepState
}

type stepSample struct {
	sample  Sample
	bucket  int64
	emitted bool
}

// stepState holds, per auth, the last sample carrying each window.
type stepState struct {
	w5, w7 *stepSample
}

func newStepReducer(step int64) *stepReducer {
	if step <= 0 {
		return nil
	}
	return &stepReducer{step: step, last: make(map[string]*stepState)}
}

func emitStep(out []Sample, s *stepSample) []Sample {
	if s == nil || s.emitted {
		return out
	}
	s.emitted = true
	return append(out, s.sample)
}

func (r *stepReducer) add(out []Sample, s Sample) []Sample {
	cur := &stepSample{sample: s, bucket: s.T.Unix() / r.step}
	state, seen := r.last[s.AuthID]
	if !seen {
		// The first sample has no predecessor to detect a change against, so it
		// is always kept: a tail query may start right before a new window.
		state = &stepState{}
		r.last[s.AuthID] = state
		out = emitStep(out, cur)
	}
	if carries5(s) {
		out = addStepWindow(out, &state.w5, cur, changed5)
	}
	if carries7(s) {
		out = addStepWindow(out, &state.w7, cur, changed7)
	}
	return out
}

// addStepWindow applies the bucket and boundary rules for one window. The previous
// sample carrying the window is kept when it ends its bucket or precedes a change;
// the current one is kept when it starts a new window.
func addStepWindow(out []Sample, last **stepSample, cur *stepSample, changed func(prev, s Sample) bool) []Sample {
	if prev := *last; prev != nil {
		boundary := changed(prev.sample, cur.sample)
		if boundary || cur.bucket != prev.bucket {
			out = emitStep(out, prev)
		}
		if boundary {
			out = emitStep(out, cur)
		}
	}
	*last = cur
	return out
}

// flush emits the last sample carrying each window of every auth.
func (r *stepReducer) flush(out []Sample) []Sample {
	for _, state := range r.last {
		out = emitStep(out, state.w5)
		out = emitStep(out, state.w7)
	}
	return out
}

func (st *Store) readDay(day time.Time, visit func(Sample)) error {
	f, errOpen := os.Open(st.dayPath(day))
	if errOpen != nil {
		if os.IsNotExist(errOpen) {
			return nil
		}
		return fmt.Errorf("open quota history file: %w", errOpen)
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			log.Errorf("quota history: close day file: %v", errClose)
		}
	}()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	for scanner.Scan() {
		var s Sample
		// Skip malformed lines, including a line still being appended.
		if errDecode := json.Unmarshal(scanner.Bytes(), &s); errDecode != nil || s.T.IsZero() {
			continue
		}
		visit(s)
	}
	if errScan := scanner.Err(); errScan != nil {
		return fmt.Errorf("read quota history file: %w", errScan)
	}
	return nil
}

// Prune deletes day files older than the retention period relative to now.
func (st *Store) Prune(now time.Time) error {
	days, errDays := st.days()
	if errDays != nil {
		return errDays
	}
	cutoff := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -retentionDays)
	for _, day := range days {
		if !day.Before(cutoff) {
			continue
		}
		if errRemove := os.Remove(st.dayPath(day)); errRemove != nil && !os.IsNotExist(errRemove) {
			return fmt.Errorf("remove quota history file: %w", errRemove)
		}
	}
	return nil
}

// Recorder throttles observations per auth and hands them to a background
// writer. Record never blocks: when the queue is full the sample is dropped.
type Recorder struct {
	store   *Store
	queue   chan Sample
	nowFunc func() time.Time

	mu         sync.Mutex
	last       map[string]lastRecorded
	dropped    int
	dropWarnAt time.Time
}

var _ coreauth.QuotaHistoryRecorder = (*Recorder)(nil)

// NewRecorder returns a recorder writing to store. Call Run to start writing.
func NewRecorder(store *Store) *Recorder {
	return &Recorder{
		store:   store,
		queue:   make(chan Sample, queueSize),
		nowFunc: time.Now,
		last:    make(map[string]lastRecorded),
	}
}

// lastRecorded is the last recorded sample carrying each window, for the throttle.
type lastRecorded struct {
	w5, w7 *Sample
}

// Record captures the rate-limit headers of a Claude response. At most one
// sample per auth is kept per throttle interval, except that a change of either
// window's reset time is recorded immediately.
func (r *Recorder) Record(auth *coreauth.Auth, headers http.Header, now time.Time) {
	if r == nil {
		return
	}
	s, ok := Extract(headers)
	if !ok {
		return
	}
	r.RecordSample(auth, s, now)
}

// RecordSample records a sample built elsewhere, such as from the usage
// endpoint, for a Claude auth with the same throttle as Record. T and AuthID
// are set from now and auth. It never blocks.
func (r *Recorder) RecordSample(auth *coreauth.Auth, s Sample, now time.Time) {
	if r == nil || auth == nil || auth.ID == "" || !strings.EqualFold(strings.TrimSpace(auth.Provider), "claude") {
		return
	}
	if s.U5 == nil && s.U7 == nil {
		return
	}
	if now.IsZero() {
		now = r.nowFunc()
	}
	s.T = now.UTC().Truncate(time.Second)
	s.AuthID = auth.ID

	r.mu.Lock()
	defer r.mu.Unlock()
	last := r.last[s.AuthID]
	// Each window is throttled on its own: a sample is recorded when any window it
	// carries is new for this auth, changed, or was last recorded over a minute ago.
	// A usage refresh that brings the first 7d reading thus gets through even while
	// 5h-only header samples arrive every few seconds.
	due := func(prev *Sample, changed func(prev, s Sample) bool) bool {
		return prev == nil || now.Sub(prev.T) >= throttleInterval || changed(*prev, s)
	}
	record := (carries5(s) && due(last.w5, changed5)) || (carries7(s) && due(last.w7, changed7))
	if !record {
		return
	}
	select {
	case r.queue <- s:
		recorded := s
		if carries5(s) {
			last.w5 = &recorded
		}
		if carries7(s) {
			last.w7 = &recorded
		}
		r.last[s.AuthID] = last
	default:
		r.dropped++
		if now.Sub(r.dropWarnAt) >= dropWarnInterval {
			log.Warnf("quota history: write queue full, dropped %d sample(s)", r.dropped)
			r.dropped = 0
			r.dropWarnAt = now
		}
	}
}

// Query reads recorded samples reduced to step seconds (0 for raw), capped at
// MaxQuerySamples.
func (r *Recorder) Query(since, until time.Time, authIDs []string, step int64) ([]Sample, bool, error) {
	return r.store.Query(since, until, authIDs, step, MaxQuerySamples)
}

// Run writes queued samples and prunes old day files until ctx is done.
func (r *Recorder) Run(ctx context.Context) {
	r.prune()
	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-r.queue:
			if errAppend := r.store.Append(s); errAppend != nil {
				log.Warnf("quota history: %v", errAppend)
			}
		case <-ticker.C:
			r.prune()
		}
	}
}

func (r *Recorder) prune() {
	if errPrune := r.store.Prune(r.nowFunc()); errPrune != nil {
		log.Warnf("quota history: prune: %v", errPrune)
	}
}
