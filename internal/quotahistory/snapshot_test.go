package quotahistory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func entryString(e SnapshotEntry) string {
	b, _ := json.Marshal(e)
	return string(b)
}

func TestSnapshotApplySample(t *testing.T) {
	sn := NewSnapshot(t.TempDir())
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)

	sn.ApplySample(Sample{T: base, AuthID: "a", U5: float(0.1), R5: i64(1793134800), S5: "allowed", U7: float(0.2), R7: i64(1793536000), S7: "allowed"})
	// A 5h-only sample updates the 5h window and observed_at, and keeps 7d.
	sn.ApplySample(Sample{T: base.Add(time.Minute), AuthID: "a", U5: float(0.3), R5: i64(1793134800)})
	got := sn.Accounts()["a"]
	if want := `{"observed_at":"2026-10-27T11:01:00Z","u5":0.3,"r5":1793134800,"s5":"allowed","u7":0.2,"r7":1793536000,"s7":"allowed"}`; entryString(got) != want {
		t.Fatalf("after 5h-only sample = %s, want %s", entryString(got), want)
	}

	// An older sample is ignored.
	sn.ApplySample(Sample{T: base.Add(30 * time.Second), AuthID: "a", U5: float(0.9), U7: float(0.9)})
	if got := sn.Accounts()["a"]; *got.U5 != 0.3 || *got.U7 != 0.2 || !got.ObservedAt.Equal(base.Add(time.Minute)) {
		t.Fatalf("older sample applied: %s", entryString(got))
	}

	// A newer sample wins; a new 7d window without a status clears the old status,
	// and a 5h reading without reset or status in the same window keeps both.
	sn.ApplySample(Sample{T: base.Add(2 * time.Minute), AuthID: "a", U5: float(0.35), U7: float(0), R7: i64(1793600000)})
	got = sn.Accounts()["a"]
	if want := `{"observed_at":"2026-10-27T11:02:00Z","u5":0.35,"r5":1793134800,"s5":"allowed","u7":0,"r7":1793600000}`; entryString(got) != want {
		t.Fatalf("after newer sample = %s, want %s", entryString(got), want)
	}

	// A utilization drop without a reset reading is a new window too.
	sn.ApplySample(Sample{T: base.Add(3 * time.Minute), AuthID: "a", U5: float(0.05)})
	if got := sn.Accounts()["a"]; got.U5 == nil || *got.U5 != 0.05 || got.R5 != nil || got.S5 != "" {
		t.Fatalf("after 5h drop = %s", entryString(got))
	}

	sn.ApplySample(Sample{T: base, AuthID: "", U5: float(0.1)})
	sn.ApplySample(Sample{T: base, AuthID: "b"})
	if accounts := sn.Accounts(); len(accounts) != 1 {
		t.Fatalf("ignored samples created entries: %v", accounts)
	}
}

func TestRecorderRecordUsage(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	auth := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
	base := time.Date(2026, 10, 27, 9, 15, 0, 0, time.UTC)
	body := "{\n  \"five_hour\": {\"utilization\": 42, \"resets_at\": \"2026-10-27T15:00:00Z\"},\n" +
		"  \"seven_day_opus\": null, \"extra_usage\": {\"is_enabled\": false}\n}"

	r.RecordUsage(auth, []byte(body), base.Add(400*time.Millisecond))
	for _, bad := range []string{`[1,2]`, `"text"`, `null`, `{"five_hour":`, ``, `{"pad":"` + strings.Repeat("x", MaxUsageBytes) + `"}`} {
		r.RecordUsage(auth, []byte(bad), base.Add(time.Hour))
	}
	r.RecordUsage(&coreauth.Auth{ID: "codex-a", Provider: "codex"}, []byte(`{}`), base)
	r.RecordUsage(nil, []byte(`{}`), base)
	if len(r.queue) != 1 {
		t.Fatalf("queued items = %d, want 1", len(r.queue))
	}
	r.handle(<-r.queue)

	// An older refresh does not replace a newer one.
	r.RecordUsage(auth, []byte(`{"five_hour":null}`), base.Add(-time.Minute))
	r.handle(<-r.queue)

	got := r.QuotaSnapshot()["claude-a"]
	want := `{"refreshed_at":"2026-10-27T09:15:00Z","usage":{"five_hour":{"utilization":42,"resets_at":"2026-10-27T15:00:00Z"},` +
		`"seven_day_opus":null,"extra_usage":{"is_enabled":false}}}`
	if entryString(got) != want {
		t.Fatalf("usage entry = %s, want %s", entryString(got), want)
	}

	// A sample keeps the stored usage.
	r.handle(queueItem{sample: Sample{T: base.Add(time.Minute), AuthID: "claude-a", U5: float(0.5)}})
	if got := r.QuotaSnapshot()["claude-a"]; got.RefreshedAt == nil || len(got.Usage) == 0 || got.U5 == nil {
		t.Fatalf("entry after sample = %s", entryString(got))
	}
}

func TestRecordUsageNeverBlocksWhenQueueFull(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	h := claudeHeaders("0.1", "", "", "")
	for i := 0; i < queueSize; i++ {
		r.Record(&coreauth.Auth{ID: "claude-" + strconv.Itoa(i), Provider: "claude"}, h, base)
	}
	auth := &coreauth.Auth{ID: "claude-usage", Provider: "claude"}
	for i := 0; i < 5; i++ {
		r.RecordUsage(auth, []byte(`{}`), base)
	}
	// The first drop logs a warning and resets the counter.
	if len(r.queue) != queueSize || r.dropped != 4 {
		t.Fatalf("queue=%d dropped=%d", len(r.queue), r.dropped)
	}
}

func readSnapshotFile(t *testing.T, dir string) map[string]SnapshotEntry {
	t.Helper()
	data, errRead := os.ReadFile(filepath.Join(dir, SnapshotFileName))
	if errRead != nil {
		t.Fatal(errRead)
	}
	var file snapshotFile
	if errDecode := json.Unmarshal(data, &file); errDecode != nil {
		t.Fatalf("decode %s: %v", data, errDecode)
	}
	return file.Accounts
}

func TestSnapshotFlushDebounce(t *testing.T) {
	dir := t.TempDir()
	r := NewRecorder(NewStore(dir))
	clock := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	r.nowFunc = func() time.Time { return clock }
	sample := func(u float64) queueItem {
		return queueItem{sample: Sample{T: clock, AuthID: "claude-a", U5: float(u)}}
	}

	r.flushSnapshot(false)
	if _, errStat := os.Stat(filepath.Join(dir, SnapshotFileName)); !os.IsNotExist(errStat) {
		t.Fatalf("clean snapshot written: %v", errStat)
	}

	// The first change after a quiet period is written at once.
	r.handle(sample(0.1))
	r.flushSnapshot(false)
	if got := readSnapshotFile(t, dir)["claude-a"]; *got.U5 != 0.1 {
		t.Fatalf("first write = %s", entryString(got))
	}

	// A burst inside the debounce interval is held back.
	for _, u := range []float64{0.2, 0.3, 0.4} {
		clock = clock.Add(time.Second)
		r.handle(sample(u))
		r.flushSnapshot(false)
	}
	clock = clock.Add(time.Second) // 4s after the first write
	r.flushSnapshot(false)
	if got := readSnapshotFile(t, dir)["claude-a"]; *got.U5 != 0.1 {
		t.Fatalf("burst written early = %s", entryString(got))
	}
	clock = clock.Add(time.Second) // 5s after the first write
	r.flushSnapshot(false)
	if got := readSnapshotFile(t, dir)["claude-a"]; *got.U5 != 0.4 {
		t.Fatalf("debounced write = %s", entryString(got))
	}

	// Shutdown forces a pending change out.
	clock = clock.Add(time.Second)
	r.handle(sample(0.5))
	r.flushSnapshot(false)
	r.flushSnapshot(true)
	if got := readSnapshotFile(t, dir)["claude-a"]; *got.U5 != 0.5 {
		t.Fatalf("forced write = %s", entryString(got))
	}
}

func TestSnapshotAtomicWriteAndReload(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	sn := NewSnapshot(dir)
	base := time.Date(2026, 10, 27, 11, 58, 3, 0, time.UTC)
	sn.ApplySample(Sample{T: base, AuthID: "claude-a", U5: float(0.42), R5: i64(1793116800), S5: "allowed", U7: float(0.11), R7: i64(1793536000), S7: "allowed"})
	sn.ApplyUsage("claude-a", json.RawMessage(`{"five_hour":{"utilization":42}}`), base.Add(-time.Hour))
	sn.ApplySample(Sample{T: base, AuthID: "claude-b", U7: float(0.5)})
	if errFlush := sn.Flush(base, false); errFlush != nil {
		t.Fatal(errFlush)
	}

	info, errStat := os.Stat(filepath.Join(dir, SnapshotFileName))
	if errStat != nil {
		t.Fatal(errStat)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("snapshot mode = %o", perm)
	}
	entries, errRead := os.ReadDir(dir)
	if errRead != nil || len(entries) != 1 {
		t.Fatalf("dir entries = %v err=%v, want only the snapshot", entries, errRead)
	}

	reloaded := NewSnapshot(dir)
	loaded, errLoad := reloaded.Load()
	if errLoad != nil || !loaded {
		t.Fatalf("load = %v %v", loaded, errLoad)
	}
	want, got := sn.Accounts(), reloaded.Accounts()
	if len(got) != 2 || entryString(got["claude-a"]) != entryString(want["claude-a"]) || entryString(got["claude-b"]) != entryString(want["claude-b"]) {
		t.Fatalf("reloaded = %v, want %v", got, want)
	}
	// A reloaded snapshot is clean until it changes.
	if errFlush := reloaded.Flush(base.Add(time.Hour), true); errFlush != nil {
		t.Fatal(errFlush)
	}

	if loaded, errLoad := NewSnapshot(t.TempDir()).Load(); loaded || errLoad != nil {
		t.Fatalf("missing file load = %v %v", loaded, errLoad)
	}
	if errWrite := os.WriteFile(filepath.Join(dir, SnapshotFileName), []byte(`{"accounts":`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if loaded, errLoad := NewSnapshot(dir).Load(); loaded || errLoad == nil {
		t.Fatalf("corrupt file load = %v %v", loaded, errLoad)
	}
}

func TestSnapshotSeedFromDayFiles(t *testing.T) {
	st := NewStore(t.TempDir())
	now := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	appendSamples(t, st,
		// Outside the seven seeded days.
		Sample{T: now.AddDate(0, 0, -7), AuthID: "claude-old", U5: float(0.9)},
		Sample{T: now.AddDate(0, 0, -6).Add(-time.Hour), AuthID: "claude-a", U7: float(0.3), R7: i64(1793536000), S7: "allowed"},
		Sample{T: now.AddDate(0, 0, -1), AuthID: "claude-a", U5: float(0.1), R5: i64(1793134800)},
		Sample{T: now.Add(-time.Hour), AuthID: "claude-a", U5: float(0.2), R5: i64(1793134800)},
		Sample{T: now.Add(-2 * time.Hour), AuthID: "claude-b", U7: float(0.6)},
	)
	sn := NewSnapshot(st.dir)
	if errSeed := sn.Seed(st, now); errSeed != nil {
		t.Fatal(errSeed)
	}
	got := sn.Accounts()
	if len(got) != 2 {
		t.Fatalf("seeded accounts = %v", got)
	}
	if want := `{"observed_at":"2026-10-27T10:00:00Z","u5":0.2,"r5":1793134800,"u7":0.3,"r7":1793536000,"s7":"allowed"}`; entryString(got["claude-a"]) != want {
		t.Fatalf("claude-a = %s, want %s", entryString(got["claude-a"]), want)
	}
	if want := `{"observed_at":"2026-10-27T09:00:00Z","u7":0.6}`; entryString(got["claude-b"]) != want {
		t.Fatalf("claude-b = %s, want %s", entryString(got["claude-b"]), want)
	}
}

func TestRecorderRunSeedsAndFlushesOnShutdown(t *testing.T) {
	st := NewStore(t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	appendSamples(t, st, Sample{T: now.Add(-time.Minute), AuthID: "claude-a", U5: float(0.25)})
	r := NewRecorder(st)
	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for len(r.QuotaSnapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("snapshot was not seeded")
		}
		runtime.Gosched()
	}
	cancel()
	<-r.Done()
	if got := readSnapshotFile(t, st.dir)["claude-a"]; got.U5 == nil || *got.U5 != 0.25 {
		t.Fatalf("flushed snapshot = %s", entryString(got))
	}
}

func TestRecorderRunWritesQueueOnShutdown(t *testing.T) {
	st := NewStore(filepath.Join(t.TempDir(), DirName))
	clock := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	r := NewRecorder(st)
	r.nowFunc = func() time.Time { return clock }
	authA := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
	authB := &coreauth.Auth{ID: "claude-b", Provider: "claude"}
	r.RecordSample(authA, Sample{U5: float(0.1), R5: i64(1793134800)}, clock)
	r.RecordSample(authB, Sample{U7: float(0.4), R7: i64(1793536000)}, clock)
	r.RecordUsage(authA, []byte(`{"five_hour":{"utilization":10}}`), clock)
	if len(r.queue) != 3 {
		t.Fatalf("queued items = %d, want 3", len(r.queue))
	}
	select {
	case <-r.Done():
		t.Fatal("Done closed before Run")
	default:
	}

	// Run starts with ctx already cancelled, so it may pick ctx.Done before any
	// queued item; every item must still be written.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go r.Run(ctx)
	select {
	case <-r.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done was not closed after Run returned")
	}

	samples, _, errQuery := st.Query(clock.Add(-time.Hour), clock.Add(time.Hour), nil, 0, 0)
	if errQuery != nil {
		t.Fatal(errQuery)
	}
	assertSamples(t, samples, "claude-a@2026-10-27T11:00:00", "claude-b@2026-10-27T11:00:00")
	got := readSnapshotFile(t, st.dir)
	if a := got["claude-a"]; a.U5 == nil || *a.U5 != 0.1 || string(a.Usage) != `{"five_hour":{"utilization":10}}` {
		t.Fatalf("claude-a = %s", entryString(a))
	}
	if b := got["claude-b"]; b.U7 == nil || *b.U7 != 0.4 {
		t.Fatalf("claude-b = %s", entryString(b))
	}
}

func TestRecorderLoadAppliesNewerDayFileSamples(t *testing.T) {
	st := NewStore(filepath.Join(t.TempDir(), DirName))
	clock := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	flushed := clock.Add(-10 * time.Minute)
	stored := NewSnapshot(st.dir)
	stored.ApplySample(Sample{T: flushed, AuthID: "claude-a", U5: float(0.2), R5: i64(1793134800), S5: "allowed", U7: float(0.3), R7: i64(1793536000), S7: "allowed"})
	stored.ApplyUsage("claude-a", json.RawMessage(`{"five_hour":{"utilization":20}}`), flushed)
	if errFlush := stored.Flush(flushed, true); errFlush != nil {
		t.Fatal(errFlush)
	}
	appendSamples(t, st,
		// Already in the snapshot file: an older reading that must not win.
		Sample{T: flushed.Add(-time.Hour), AuthID: "claude-a", U5: float(0.9), R5: i64(1793134800)},
		// Written after the last flush and lost from the file on shutdown.
		Sample{T: flushed.Add(5 * time.Minute), AuthID: "claude-a", U5: float(0.25), R5: i64(1793134800)},
		Sample{T: flushed.Add(6 * time.Minute), AuthID: "claude-b", U7: float(0.5)},
	)

	r := NewRecorder(st)
	r.nowFunc = func() time.Time { return clock }
	r.loadSnapshot()
	got := r.QuotaSnapshot()
	want := `{"observed_at":"2026-10-27T10:55:00Z","u5":0.25,"r5":1793134800,"s5":"allowed","u7":0.3,"r7":1793536000,"s7":"allowed",` +
		`"refreshed_at":"2026-10-27T10:50:00Z","usage":{"five_hour":{"utilization":20}}}`
	if entryString(got["claude-a"]) != want {
		t.Fatalf("claude-a = %s, want %s", entryString(got["claude-a"]), want)
	}
	if want := `{"observed_at":"2026-10-27T10:56:00Z","u7":0.5}`; entryString(got["claude-b"]) != want {
		t.Fatalf("claude-b = %s, want %s", entryString(got["claude-b"]), want)
	}
}

func TestSnapshotFilesAreNotDayFilesOrJSON(t *testing.T) {
	if strings.HasSuffix(SnapshotFileName, ".json") || strings.HasSuffix(SnapshotFileName, fileExt) {
		t.Fatalf("snapshot file name %q would be loaded as a credential or a day file", SnapshotFileName)
	}
	st := NewStore(t.TempDir())
	now := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	appendSamples(t, st, Sample{T: now, AuthID: "claude-a", U5: float(0.1)})
	sn := NewSnapshot(st.dir)
	sn.ApplySample(Sample{T: now, AuthID: "claude-a", U5: float(0.1)})
	if errFlush := sn.Flush(now, true); errFlush != nil {
		t.Fatal(errFlush)
	}
	days, errDays := st.days()
	if errDays != nil || len(days) != 1 {
		t.Fatalf("days = %v err=%v, want only the day file", days, errDays)
	}
	// Prune far in the future removes the day file and keeps the snapshot.
	if errPrune := st.Prune(now.AddDate(1, 0, 0)); errPrune != nil {
		t.Fatal(errPrune)
	}
	if _, errStat := os.Stat(filepath.Join(st.dir, SnapshotFileName)); errStat != nil {
		t.Fatalf("snapshot removed by prune: %v", errStat)
	}
}

func TestRecordSampleNowBypassesThrottle(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	clock := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	header := &coreauth.Auth{ID: "claude-header", Provider: "claude"}
	usage := &coreauth.Auth{ID: "claude-usage", Provider: "claude"}
	reading := Sample{U5: float(0.1), R5: i64(1793134800), U7: float(0.2), R7: i64(1793536000)}
	r.RecordSample(header, reading, clock)
	r.RecordSample(header, reading, clock.Add(30*time.Second))
	r.RecordSampleNow(usage, reading, clock)
	r.RecordSampleNow(usage, reading, clock.Add(30*time.Second))
	// A forced sample still counts for the throttle of later header samples.
	r.RecordSample(usage, reading, clock.Add(60*time.Second))

	var got []Sample
	for n := len(r.queue); n > 0; n-- {
		got = append(got, (<-r.queue).sample)
	}
	assertSamples(t, got, "claude-header@2026-10-27T11:00:00", "claude-usage@2026-10-27T11:00:00", "claude-usage@2026-10-27T11:00:30")
}

func TestRecordSampleNowNeverBlocksWhenQueueFull(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	h := claudeHeaders("0.1", "", "", "")
	for i := 0; i < queueSize; i++ {
		r.Record(&coreauth.Auth{ID: "claude-" + strconv.Itoa(i), Provider: "claude"}, h, base)
	}
	auth := &coreauth.Auth{ID: "claude-usage", Provider: "claude"}
	r.RecordSampleNow(auth, Sample{U7: float(0.2), R7: i64(1793536000)}, base)
	if len(r.queue) != queueSize {
		t.Fatalf("queue=%d", len(r.queue))
	}
	if _, ok := r.last[auth.ID]; ok {
		t.Fatal("dropped sample updated the throttle state")
	}
}

func TestRecorderStartOutlivesParentContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := NewStore(filepath.Join(t.TempDir(), DirName))
		clock := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
		r := NewRecorder(st)
		r.nowFunc = func() time.Time { return clock }
		parent, cancelParent := context.WithCancel(context.Background())
		stop := r.Start(parent)

		// The owner's run context ends first, as it does at the start of a
		// service shutdown; the writer must keep running until stop.
		cancelParent()
		synctest.Wait()
		select {
		case <-r.Done():
			t.Fatal("recorder stopped with its parent context")
		default:
		}

		auth := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
		r.RecordSample(auth, Sample{U5: float(0.3), R5: i64(1793134800)}, clock)
		r.RecordUsage(auth, []byte(`{"five_hour":{"utilization":30}}`), clock)
		stop()
		<-r.Done()

		samples, _, errQuery := st.Query(clock.Add(-time.Hour), clock.Add(time.Hour), nil, 0, 0)
		if errQuery != nil {
			t.Fatal(errQuery)
		}
		assertSamples(t, samples, "claude-a@2026-10-27T11:00:00")
		if a := readSnapshotFile(t, st.dir)["claude-a"]; a.U5 == nil || *a.U5 != 0.3 || string(a.Usage) != `{"five_hour":{"utilization":30}}` {
			t.Fatalf("claude-a = %s", entryString(a))
		}
	})
}

func TestSnapshotDropsWindowEndedWithoutNewerReading(t *testing.T) {
	sn := NewSnapshot(t.TempDir())
	oct31 := time.Date(2026, 10, 31, 21, 8, 25, 0, time.UTC)
	reset5 := time.Date(2026, 10, 31, 21, 50, 0, 0, time.UTC).Unix()
	sn.ApplySample(Sample{T: oct31, AuthID: "ultra", U5: float(1), R5: i64(reset5), S5: "rejected", U7: float(0.17), R7: i64(1794060000)})

	// A weekly-only response while the 5h window is still open (Anthropic left the
	// 5h headers out) keeps the 5h reading: it still describes the open window.
	sn.ApplySample(Sample{T: oct31.Add(10 * time.Minute), AuthID: "ultra", U7: float(0.18), R7: i64(1794060000)})
	if got := sn.Accounts()["ultra"]; got.U5 == nil || *got.U5 != 1 || got.S5 != "rejected" {
		t.Fatalf("open 5h window dropped: %s", entryString(got))
	}

	// After the 5h reset, a weekly-only response means the account has no 5h window.
	nov1 := time.Date(2026, 11, 1, 17, 11, 22, 0, time.UTC)
	sn.ApplySample(Sample{T: nov1, AuthID: "ultra", U7: float(0.17), R7: i64(1794060000), S7: "allowed"})
	got := sn.Accounts()["ultra"]
	if want := `{"observed_at":"2026-11-01T17:11:22Z","u7":0.17,"r7":1794060000,"s7":"allowed"}`; entryString(got) != want {
		t.Fatalf("after reset = %s, want %s", entryString(got), want)
	}

	// The same holds for a weekly window on an account that only reports 5h.
	sn.ApplySample(Sample{T: oct31, AuthID: "team", U5: float(0.2), R5: i64(reset5), U7: float(0.3), R7: i64(oct31.Add(time.Hour).Unix())})
	sn.ApplySample(Sample{T: oct31.Add(2 * time.Hour), AuthID: "team", U5: float(0.1), R5: i64(oct31.Add(5 * time.Hour).Unix())})
	if got := sn.Accounts()["team"]; got.U7 != nil || got.R7 != nil || got.U5 == nil || *got.U5 != 0.1 {
		t.Fatalf("team entry = %s", entryString(got))
	}
}

func TestSnapshotIdleWindowAfterReset(t *testing.T) {
	sn := NewSnapshot(t.TempDir())
	nov10 := time.Date(2026, 11, 10, 13, 43, 0, 0, time.UTC)
	reset5 := time.Date(2026, 11, 10, 15, 30, 0, 0, time.UTC).Unix()
	sn.ApplySample(Sample{T: nov10, AuthID: "team", U5: float(0.45), R5: i64(reset5), U7: float(1), R7: i64(1794650400), S7: "rejected"})

	// A usage refresh after the 5h reset reports the idle window (utilization 0,
	// no reset): the entry keeps a 5h reading of 0 with no reset or status. The
	// weekly drop (a reset grant) clears the old rejected status.
	nov11 := time.Date(2026, 11, 11, 8, 18, 45, 0, time.UTC)
	sn.ApplySample(Sample{T: nov11, AuthID: "team", U5: float(0), U7: float(0.63), R7: i64(1794650400)})
	got := sn.Accounts()["team"]
	if want := `{"observed_at":"2026-11-11T08:18:45Z","u5":0,"u7":0.63,"r7":1794650400}`; entryString(got) != want {
		t.Fatalf("idle entry = %s, want %s", entryString(got), want)
	}
}

func TestSnapshotLoadDropsEndedWindow(t *testing.T) {
	dir := t.TempDir()
	stale := `{"accounts":{"ultra":{"observed_at":"2026-11-02T11:04:56Z","u5":1,"r5":1793483400,"s5":"rejected","u7":0.28,"r7":1794059999,"s7":"allowed"}}}`
	if errWrite := os.WriteFile(filepath.Join(dir, SnapshotFileName), []byte(stale), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	sn := NewSnapshot(dir)
	if _, errLoad := sn.Load(); errLoad != nil {
		t.Fatal(errLoad)
	}
	got := sn.Accounts()["ultra"]
	if want := `{"observed_at":"2026-11-02T11:04:56Z","u7":0.28,"r7":1794059999,"s7":"allowed"}`; entryString(got) != want {
		t.Fatalf("loaded = %s, want %s", entryString(got), want)
	}
}

func TestParsePlan(t *testing.T) {
	plan, ok := ParsePlan([]byte(`{"account":{"has_claude_max":true},"organization":{"organization_type":"claude_max","rate_limit_tier":"default_claude_max_20x","billing_type":"stripe_subscription"}}`))
	if !ok || plan.OrganizationType != "claude_max" || plan.RateLimitTier != "default_claude_max_20x" {
		t.Fatalf("plan = %+v ok = %v", plan, ok)
	}
	for _, body := range []string{`{"account":{}}`, `{"organization":{"organization_type":"  "}}`, `not json`, ``} {
		if _, ok := ParsePlan([]byte(body)); ok {
			t.Fatalf("ParsePlan(%q) reported a plan", body)
		}
	}
}

func TestRecorderRecordProfileKeepsNewestPlanAcrossReload(t *testing.T) {
	dir := t.TempDir()
	r := NewRecorder(NewStore(dir))
	auth := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
	base := time.Date(2026, 11, 2, 11, 0, 0, 0, time.UTC)
	r.RecordProfile(auth, []byte(`{"organization":{"organization_type":"claude_max","rate_limit_tier":"default_claude_max_5x"}}`), base.Add(time.Minute))
	// An older profile response never replaces a newer one.
	r.RecordProfile(auth, []byte(`{"organization":{"organization_type":"claude_team","rate_limit_tier":"default_claude_max_5x"}}`), base)
	r.RecordProfile(&coreauth.Auth{ID: "codex-a", Provider: "codex"}, []byte(`{"organization":{"organization_type":"x"}}`), base)
	r.drainQueue()
	if errFlush := r.snapshot.Flush(base, true); errFlush != nil {
		t.Fatal(errFlush)
	}

	reloaded := NewSnapshot(dir)
	if _, errLoad := reloaded.Load(); errLoad != nil {
		t.Fatal(errLoad)
	}
	accounts := reloaded.Accounts()
	if want := `{"plan":{"organization_type":"claude_max","rate_limit_tier":"default_claude_max_5x","profiled_at":"2026-11-02T11:01:00Z"}}`; len(accounts) != 1 || entryString(accounts["claude-a"]) != want {
		t.Fatalf("reloaded = %v, want claude-a %s", accounts, want)
	}
}
