package quotahistory

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func claudeHeaders(u5, r5, u7, r7 string) http.Header {
	h := http.Header{}
	set := func(name, value string) {
		if value != "" {
			h.Set(name, value)
		}
	}
	set("Anthropic-Ratelimit-Unified-5h-Utilization", u5)
	set("Anthropic-Ratelimit-Unified-5h-Reset", r5)
	set("Anthropic-Ratelimit-Unified-7d-Utilization", u7)
	set("Anthropic-Ratelimit-Unified-7d-Reset", r7)
	return h
}

func TestExtract(t *testing.T) {
	h := claudeHeaders("0.42", "1761580800", "0.11", "1762000000")
	h.Set("Anthropic-Ratelimit-Unified-5h-Status", " Allowed ")
	h["anthropic-ratelimit-unified-7d-status"] = []string{"allowed_warning"}
	s, ok := Extract(h)
	if !ok {
		t.Fatal("expected sample")
	}
	if s.U5 == nil || *s.U5 != 0.42 || s.U7 == nil || *s.U7 != 0.11 {
		t.Fatalf("utilization = %v/%v", s.U5, s.U7)
	}
	if s.R5 == nil || *s.R5 != 1761580800 || s.R7 == nil || *s.R7 != 1762000000 {
		t.Fatalf("reset = %v/%v", s.R5, s.R7)
	}
	if s.S5 != "allowed" || s.S7 != "allowed_warning" {
		t.Fatalf("status = %q/%q", s.S5, s.S7)
	}

	if _, ok := Extract(http.Header{"Retry-After": {"10"}}); ok {
		t.Fatal("absent utilization should not produce a sample")
	}
	if _, ok := Extract(nil); ok {
		t.Fatal("nil headers should not produce a sample")
	}
	if _, ok := Extract(claudeHeaders("abc", "1761580800", "NaN", "")); ok {
		t.Fatal("malformed utilization should not produce a sample")
	}
	if _, ok := Extract(claudeHeaders("1.5", "", "-0.1", "")); ok {
		t.Fatal("out of range utilization should not produce a sample")
	}

	s, ok = Extract(claudeHeaders("", "", "1", "2026-10-27T12:00:00Z"))
	if !ok || s.U5 != nil || s.R5 != nil || s.U7 == nil || *s.U7 != 1 {
		t.Fatalf("7d-only sample = %+v ok=%v", s, ok)
	}
	if s.R7 == nil || *s.R7 != time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("RFC3339 reset = %v", s.R7)
	}
	s, ok = Extract(claudeHeaders("0.5", "garbage", "2", ""))
	if !ok || s.R5 != nil || s.U7 != nil {
		t.Fatalf("partial sample = %+v ok=%v", s, ok)
	}

	encoded, errMarshal := json.Marshal(s)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	for _, key := range []string{`"r5"`, `"u7"`, `"r7"`, `"s5"`, `"s7"`} {
		if strings.Contains(string(encoded), key) {
			t.Fatalf("absent field %s serialized: %s", key, encoded)
		}
	}
}

func drain(r *Recorder) []Sample {
	var out []Sample
	for {
		select {
		case item := <-r.queue:
			if item.usage == nil {
				out = append(out, item.sample)
			}
		default:
			return out
		}
	}
}

func TestRecorderThrottle(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	auth := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
	other := &coreauth.Auth{ID: "claude-b", Provider: "claude"}
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	h := claudeHeaders("0.1", "1761580800", "0.2", "1762000000")

	r.Record(auth, h, base)
	r.Record(auth, h, base.Add(10*time.Second))
	r.Record(other, h, base.Add(10*time.Second))
	r.Record(auth, h, base.Add(59*time.Second))
	if got := drain(r); len(got) != 2 || got[0].AuthID != "claude-a" || got[1].AuthID != "claude-b" {
		t.Fatalf("first minute samples = %+v", got)
	}

	r.Record(auth, h, base.Add(60*time.Second))
	if got := drain(r); len(got) != 1 || !got[0].T.Equal(base.Add(60*time.Second)) {
		t.Fatalf("after interval samples = %+v", got)
	}

	// A new 5h reset is recorded immediately, inside the throttle interval.
	r.Record(auth, claudeHeaders("0", "1761598800", "0.2", "1762000000"), base.Add(61*time.Second))
	// So is a new 7d reset.
	r.Record(auth, claudeHeaders("0", "1761598800", "0", "1762604800"), base.Add(62*time.Second))
	r.Record(auth, claudeHeaders("0.3", "1761598800", "0", "1762604800"), base.Add(63*time.Second))
	got := drain(r)
	if len(got) != 2 || *got[0].R5 != 1761598800 || *got[1].R7 != 1762604800 {
		t.Fatalf("reset bypass samples = %+v", got)
	}

	r.Record(&coreauth.Auth{ID: "codex-a", Provider: "codex"}, h, base.Add(time.Hour))
	r.Record(nil, h, base.Add(time.Hour))
	if got := drain(r); len(got) != 0 {
		t.Fatalf("non-claude samples = %+v", got)
	}
}

func TestExtractFromUsage(t *testing.T) {
	r5 := time.Date(2026, 10, 27, 15, 0, 0, 0, time.UTC).Unix()
	r7 := time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC).Unix()
	tests := []struct {
		name   string
		body   string
		ok     bool
		u5, u7 *float64
		r5, r7 *int64
		s5, s7 string
	}{
		{
			name: "percentages with extra fields",
			body: `{"five_hour":{"utilization":42.5,"resets_at":"2026-10-27T15:00:00.394837+00:00"},` +
				`"seven_day":{"utilization":16,"resets_at":"2026-10-31T09:00:00Z"},` +
				`"seven_day_opus":{"utilization":3,"resets_at":null},"extra_usage":{"is_enabled":false}}`,
			ok: true, u5: float(0.425), r5: i64(r5), u7: float(0.16), r7: i64(r7),
		},
		{
			// The endpoint reports percentages, so a reading at or below 1 is a
			// small percentage, not a fraction.
			name: "small percentages are not fractions",
			body: `{"five_hour":{"utilization":1,"resets_at":"2026-10-27T15:00:00Z"},"seven_day":{"utilization":0.5,"resets_at":"2026-10-31T09:00:00Z"}}`,
			ok:   true, u5: float(0.01), r5: i64(r5), u7: float(0.005), r7: i64(r7),
		},
		{
			name: "full windows are rejected",
			body: `{"five_hour":{"utilization":100,"resets_at":"2026-10-27T15:00:00Z"},"seven_day":{"utilization":"100","resets_at":"2026-10-31T09:00:00Z"}}`,
			ok:   true, u5: float(1), r5: i64(r5), u7: float(1), r7: i64(r7), s5: "rejected", s7: "rejected",
		},
		{
			name: "missing seven day window",
			body: `{"five_hour":{"utilization":10,"resets_at":"2026-10-27T15:00:00Z"}}`,
			ok:   true, u5: float(0.1), r5: i64(r5),
		},
		{
			name: "null reset drops the window",
			body: `{"five_hour":{"utilization":0,"resets_at":null},"seven_day":null}`,
		},
		{
			name: "five hour out of range keeps seven day",
			body: `{"five_hour":{"utilization":150,"resets_at":"2026-10-27T15:00:00Z"},"seven_day":{"utilization":20,"resets_at":"2026-10-31T09:00:00Z"}}`,
			ok:   true, u7: float(0.2), r7: i64(1793437200),
		},
		{
			name: "invalid reset drops that window only",
			body: `{"five_hour":{"utilization":10,"resets_at":"not a time"},"seven_day":{"utilization":20,"resets_at":"2026-10-31T09:00:00Z"}}`,
			ok:   true, u7: float(0.2), r7: i64(1793437200),
		},
		{name: "null utilizations", body: `{"five_hour":{"utilization":null,"resets_at":null},"seven_day":{"utilization":null}}`},
		{name: "negative utilization", body: `{"five_hour":{"utilization":-1,"resets_at":"2026-10-27T15:00:00Z"}}`},
		{name: "no windows", body: `{"extra_usage":{"is_enabled":true}}`},
		{name: "malformed json", body: `{"five_hour":`},
		{name: "not an object", body: `[1,2]`},
		{name: "empty", body: ``},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractFromUsage([]byte(tc.body))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (sample %+v)", ok, tc.ok, got)
			}
			want := Sample{U5: tc.u5, R5: tc.r5, U7: tc.u7, R7: tc.r7, S5: tc.s5, S7: tc.s7}
			if !tc.ok {
				want = Sample{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("sample = %s, want %s", sampleString(got), sampleString(want))
			}
		})
	}
}

func sampleString(s Sample) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestRecorderRecordSample(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	auth := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	usage := Sample{U5: float(0.1), R5: i64(1761580800), U7: float(0.2), R7: i64(1762000000)}

	r.RecordSample(auth, usage, base.Add(500*time.Millisecond))
	got := drain(r)
	if len(got) != 1 || got[0].AuthID != "claude-a" || !got[0].T.Equal(base) || *got[0].U7 != 0.2 {
		t.Fatalf("first sample = %+v", got)
	}

	// A sample shares the throttle with header observations of the same auth.
	r.Record(auth, claudeHeaders("0.1", "1761580800", "0.2", "1762000000"), base.Add(10*time.Second))
	r.RecordSample(auth, usage, base.Add(30*time.Second))
	if got := drain(r); len(got) != 0 {
		t.Fatalf("throttled samples = %+v", got)
	}
	r.RecordSample(auth, usage, base.Add(60*time.Second))
	if got := drain(r); len(got) != 1 || !got[0].T.Equal(base.Add(60*time.Second)) {
		t.Fatalf("after interval samples = %+v", got)
	}

	// A new 7d reset bypasses the throttle.
	next := usage
	next.R7 = i64(1762604800)
	r.RecordSample(auth, next, base.Add(61*time.Second))
	r.RecordSample(auth, next, base.Add(62*time.Second))
	if got := drain(r); len(got) != 1 || *got[0].R7 != 1762604800 {
		t.Fatalf("reset bypass samples = %+v", got)
	}

	// A usage refresh that rounds the header reading down is the same window, so
	// it stays throttled.
	r.Record(auth, claudeHeaders("0.1234", "1761580800", "0.2", "1762604800"), base.Add(130*time.Second))
	drain(r)
	rounded := Sample{U5: float(12.3 / 100), R5: i64(1761580800)}
	r.RecordSample(auth, rounded, base.Add(140*time.Second))
	if got := drain(r); len(got) != 0 {
		t.Fatalf("rounded usage sample bypassed the throttle: %+v", got)
	}

	r.RecordSample(&coreauth.Auth{ID: "codex-a", Provider: "codex"}, usage, base.Add(time.Hour))
	r.RecordSample(nil, usage, base.Add(time.Hour))
	r.RecordSample(auth, Sample{}, base.Add(time.Hour))
	if got := drain(r); len(got) != 0 {
		t.Fatalf("ignored samples = %+v", got)
	}
}

func TestRecorderDropsWhenQueueFull(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	h := claudeHeaders("0.1", "", "", "")
	for i := 0; i < queueSize+10; i++ {
		r.Record(&coreauth.Auth{ID: "claude-" + strconv.Itoa(i), Provider: "claude"}, h, base)
	}
	// The first drop logs a warning and resets the counter.
	if len(r.queue) != queueSize || r.dropped != 9 {
		t.Fatalf("queue=%d dropped=%d", len(r.queue), r.dropped)
	}
}

func float(v float64) *float64 { return &v }

func TestStoreAppendQueryPrune(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	st := NewStore(dir)
	day1 := time.Date(2026, 10, 26, 23, 59, 30, 0, time.UTC)
	day2 := time.Date(2026, 10, 27, 0, 0, 30, 0, time.UTC)
	for _, s := range []Sample{
		{T: day2, AuthID: "b", U5: float(0.3)},
		{T: day1, AuthID: "a", U5: float(0.1)},
		{T: day2, AuthID: "a", U5: float(0.2)},
	} {
		if errAppend := st.Append(s); errAppend != nil {
			t.Fatal(errAppend)
		}
	}
	info, errStat := os.Stat(dir)
	if errStat != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("dir perm = %v err=%v", info, errStat)
	}
	fileInfo, errStat := os.Stat(filepath.Join(dir, "2026-10-27.jsonl"))
	if errStat != nil || fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("file perm = %v err=%v", fileInfo, errStat)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "2026-10-26.jsonl"))
	if string(raw) != `{"t":"2026-10-26T23:59:30Z","auth_id":"a","u5":0.1}`+"\n" {
		t.Fatalf("line = %q", raw)
	}
	// A malformed trailing line is skipped.
	f, _ := os.OpenFile(filepath.Join(dir, "2026-10-27.jsonl"), os.O_WRONLY|os.O_APPEND, 0o600)
	_, _ = f.WriteString(`{"t":"2026-10-27T00:01`)
	_ = f.Close()

	samples, truncated, errQuery := st.Query(day1.Add(-time.Hour), day2.Add(time.Hour), nil, 0, 0)
	if errQuery != nil || truncated || len(samples) != 3 {
		t.Fatalf("query = %+v truncated=%v err=%v", samples, truncated, errQuery)
	}
	if samples[0].AuthID != "a" || !samples[0].T.Equal(day1) || samples[1].AuthID != "a" || samples[2].AuthID != "b" {
		t.Fatalf("order = %+v", samples)
	}

	samples, _, _ = st.Query(day1.Add(-time.Hour), day2.Add(time.Hour), []string{"b"}, 0, 0)
	if len(samples) != 1 || samples[0].AuthID != "b" {
		t.Fatalf("auth filter = %+v", samples)
	}
	samples, _, _ = st.Query(day2, day2.Add(time.Hour), nil, 0, 0)
	if len(samples) != 2 {
		t.Fatalf("since filter = %+v", samples)
	}
	samples, truncated, _ = st.Query(day1.Add(-time.Hour), day2.Add(time.Hour), nil, 0, 2)
	if !truncated || len(samples) != 2 || !samples[0].T.Equal(day2) {
		t.Fatalf("truncation = %+v truncated=%v", samples, truncated)
	}

	old := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	if errAppend := st.Append(Sample{T: old, AuthID: "a", U5: float(0)}); errAppend != nil {
		t.Fatal(errAppend)
	}
	kept := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	if errAppend := st.Append(Sample{T: kept, AuthID: "a", U5: float(0)}); errAppend != nil {
		t.Fatal(errAppend)
	}
	if errPrune := st.Prune(time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC)); errPrune != nil {
		t.Fatal(errPrune)
	}
	if _, errOld := os.Stat(filepath.Join(dir, "2026-08-24.jsonl")); !os.IsNotExist(errOld) {
		t.Fatalf("old file not pruned: %v", errOld)
	}
	if _, errKept := os.Stat(filepath.Join(dir, "2026-08-25.jsonl")); errKept != nil {
		t.Fatalf("file within retention pruned: %v", errKept)
	}

	empty, _, errEmpty := NewStore(filepath.Join(t.TempDir(), "missing")).Query(day1, day2, nil, 0, 0)
	if errEmpty != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("missing dir query = %v err=%v", empty, errEmpty)
	}
}

func i64(v int64) *int64 { return &v }

func appendSamples(t *testing.T, st *Store, samples ...Sample) {
	t.Helper()
	for _, s := range samples {
		if errAppend := st.Append(s); errAppend != nil {
			t.Fatal(errAppend)
		}
	}
}

func sampleTimes(samples []Sample) []string {
	out := make([]string, 0, len(samples))
	for _, s := range samples {
		out = append(out, s.AuthID+"@"+s.T.Format("2006-01-02T15:04:05"))
	}
	return out
}

func assertSamples(t *testing.T, got []Sample, want ...string) {
	t.Helper()
	if gotTimes := sampleTimes(got); !reflect.DeepEqual(gotTimes, want) {
		t.Fatalf("samples = %v, want %v", gotTimes, want)
	}
}

func TestQueryStepKeepsLastSamplePerBucket(t *testing.T) {
	st := NewStore(t.TempDir())
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }
	appendSamples(t, st,
		Sample{T: at(0), AuthID: "a", R5: i64(1)},
		Sample{T: at(50), AuthID: "b", R5: i64(1)},
		Sample{T: at(100), AuthID: "a", R5: i64(1)},
		Sample{T: at(899), AuthID: "a", R5: i64(1)},
		Sample{T: at(900), AuthID: "a", R5: i64(1)},
		Sample{T: at(950), AuthID: "b", R5: i64(1)},
		Sample{T: at(1000), AuthID: "a", R5: i64(1)},
		Sample{T: at(2000), AuthID: "a", R5: i64(1)},
	)
	until := base.Add(time.Hour)

	samples, truncated, errQuery := st.Query(base, until, nil, 900, 0)
	if errQuery != nil || truncated {
		t.Fatalf("truncated=%v err=%v", truncated, errQuery)
	}
	assertSamples(t, samples,
		"a@2026-10-27T00:00:00", "b@2026-10-27T00:00:50", "a@2026-10-27T00:14:59",
		"b@2026-10-27T00:15:50", "a@2026-10-27T00:16:40", "a@2026-10-27T00:33:20")

	// Step 0 returns every raw sample.
	samples, truncated, _ = st.Query(base, until, nil, 0, 0)
	if truncated || len(samples) != 8 {
		t.Fatalf("raw = %v truncated=%v", sampleTimes(samples), truncated)
	}

	// The cap applies to the reduced output and drops the oldest.
	samples, truncated, _ = st.Query(base, until, nil, 900, 2)
	if !truncated {
		t.Fatal("expected truncated")
	}
	assertSamples(t, samples, "a@2026-10-27T00:16:40", "a@2026-10-27T00:33:20")
	samples, truncated, _ = st.Query(base, until, nil, 900, 6)
	if truncated || len(samples) != 6 {
		t.Fatalf("exact cap = %v truncated=%v", sampleTimes(samples), truncated)
	}

	samples, _, _ = st.Query(base, until, []string{"b"}, 900, 0)
	assertSamples(t, samples, "b@2026-10-27T00:00:50", "b@2026-10-27T00:15:50")
}

func TestQueryStepKeepsResetBoundaries(t *testing.T) {
	st := NewStore(t.TempDir())
	base := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	appendSamples(t, st,
		// Resets a window apart (5h, 7d) are different windows; a reset that is
		// missing on one side or a second off is not a change.
		Sample{T: at(0), AuthID: "a", R5: i64(18000), R7: i64(604800)},
		Sample{T: at(1), AuthID: "a", R5: i64(18001)},
		Sample{T: at(2), AuthID: "a", R5: i64(36000), R7: i64(604800)},
		Sample{T: at(3), AuthID: "a", R5: i64(36000)},
		Sample{T: at(4), AuthID: "a", R5: i64(36000), R7: i64(604801)},
		Sample{T: at(5), AuthID: "a", R5: i64(36000), R7: i64(1209600)},
		Sample{T: at(6), AuthID: "a", R5: i64(36000), R7: i64(1209600)},
		Sample{T: at(7), AuthID: "a", R5: i64(36000)},
	)
	// All samples fall into one hourly bucket.
	samples, truncated, errQuery := st.Query(base, base.Add(time.Hour), nil, 3600, 0)
	if errQuery != nil || truncated {
		t.Fatalf("truncated=%v err=%v", truncated, errQuery)
	}
	// 10:06 is the last sample carrying the 7d window in the bucket.
	assertSamples(t, samples,
		"a@2026-10-27T10:00:00", "a@2026-10-27T10:01:00", "a@2026-10-27T10:02:00",
		"a@2026-10-27T10:04:00", "a@2026-10-27T10:05:00", "a@2026-10-27T10:06:00",
		"a@2026-10-27T10:07:00")
}

func TestQueryStepKeepsResetBoundaryAcrossDays(t *testing.T) {
	st := NewStore(t.TempDir())
	// 2026-10-26 and 2026-10-27 share one epoch-aligned two-day bucket.
	const step = 2 * 24 * 60 * 60
	day1 := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	if day1.Unix()/step != day2.Add(23*time.Hour).Unix()/step {
		t.Fatal("test days are not in one bucket")
	}
	appendSamples(t, st,
		Sample{T: day1.Add(12 * time.Hour), AuthID: "a", R5: i64(18000)},
		Sample{T: day2.Add(-time.Minute), AuthID: "a", R5: i64(18000)},
		Sample{T: day2.Add(time.Minute), AuthID: "a", R5: i64(36000)},
		Sample{T: day2.Add(12 * time.Hour), AuthID: "a", R5: i64(36000)},
	)
	samples, _, errQuery := st.Query(day1, day2.Add(24*time.Hour), nil, step, 0)
	if errQuery != nil {
		t.Fatal(errQuery)
	}
	assertSamples(t, samples, "a@2026-10-26T12:00:00", "a@2026-10-26T23:59:00",
		"a@2026-10-27T00:01:00", "a@2026-10-27T12:00:00")
}

func TestQueryStepKeepsFirstSampleInRange(t *testing.T) {
	st := NewStore(t.TempDir())
	base := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	appendSamples(t, st,
		// Before the range: an older window, then minutes of idle time.
		Sample{T: at(-20), AuthID: "a", R5: i64(1)},
		Sample{T: at(-19), AuthID: "b", R5: i64(1)},
		// In range: each auth opens a new window mid-bucket, then keeps sampling
		// within the same 15-minute bucket.
		Sample{T: at(4), AuthID: "a", R5: i64(2)},
		Sample{T: at(5), AuthID: "b", R5: i64(2)},
		Sample{T: at(6), AuthID: "a", R5: i64(2)},
		Sample{T: at(8), AuthID: "b", R5: i64(2)},
		Sample{T: at(10), AuthID: "a", R5: i64(2)},
		Sample{T: at(12), AuthID: "b", R5: i64(2)},
	)
	since, until := at(-5), at(14)

	samples, truncated, errQuery := st.Query(since, until, nil, 900, 0)
	if errQuery != nil || truncated {
		t.Fatalf("truncated=%v err=%v", truncated, errQuery)
	}
	assertSamples(t, samples,
		"a@2026-10-27T12:04:00", "b@2026-10-27T12:05:00",
		"a@2026-10-27T12:10:00", "b@2026-10-27T12:12:00")
	raw, _, _ := st.Query(since, until, nil, 0, 0)
	if !reflect.DeepEqual(samples, referenceReduce(raw, 900)) {
		t.Fatalf("reference = %v", sampleTimes(referenceReduce(raw, 900)))
	}

	// Step 0 still returns exactly the raw in-range samples.
	assertSamples(t, raw,
		"a@2026-10-27T12:04:00", "b@2026-10-27T12:05:00", "a@2026-10-27T12:06:00",
		"b@2026-10-27T12:08:00", "a@2026-10-27T12:10:00", "b@2026-10-27T12:12:00")
}

// referenceReduce applies the step rules to all samples at once.
func referenceReduce(all []Sample, step int64) []Sample {
	byAuth := make(map[string][]Sample)
	for _, s := range all {
		byAuth[s.AuthID] = append(byAuth[s.AuthID], s)
	}
	var out []Sample
	for _, list := range byAuth {
		sort.SliceStable(list, func(i, j int) bool { return list[i].T.Before(list[j].T) })
		keep := make([]bool, len(list))
		keep[0] = true
		// Per window: the last carrier in each bucket, both carriers around a change.
		for _, w := range []struct {
			carries func(Sample) bool
			changed func(Sample, Sample) bool
		}{{carries5, changed5}, {carries7, changed7}} {
			prev := -1
			for i := range list {
				if !w.carries(list[i]) {
					continue
				}
				if prev >= 0 {
					if w.changed(list[prev], list[i]) {
						keep[prev], keep[i] = true, true
					}
					if list[i].T.Unix()/step != list[prev].T.Unix()/step {
						keep[prev] = true
					}
				}
				prev = i
			}
			if prev >= 0 {
				keep[prev] = true
			}
		}
		for i, s := range list {
			if keep[i] {
				out = append(out, s)
			}
		}
	}
	sortSamples(out)
	return out
}

const (
	genDays  = 10
	genAuths = 6
	// genResetPhase is the minute within the 5h window at which auth 0's window
	// resets; auth i resets genResetPhase+i minutes in. Those minutes are never
	// the first or last of a 15-minute bucket, so each reset adds two samples.
	genResetPhase = 7
)

// writeGeneratedHistory writes genDays day files with one sample per minute per
// auth. Each auth's 5h reset changes every 300 minutes, and every auth's 7d
// reset changes once at the start of day 5.
func writeGeneratedHistory(tb testing.TB, dir string) time.Time {
	tb.Helper()
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		tb.Fatal(errMkdir)
	}
	for day := 0; day < genDays; day++ {
		f, errCreate := os.Create(filepath.Join(dir, start.AddDate(0, 0, day).Format(dayLayout)+fileExt))
		if errCreate != nil {
			tb.Fatal(errCreate)
		}
		w := bufio.NewWriter(f)
		for m := day * 1440; m < (day+1)*1440; m++ {
			for i := 0; i < genAuths; i++ {
				phase := genResetPhase + i
				window := int64((m + 300 - phase) / 300)
				r5 := start.Unix() + (window*300+int64(phase))*60
				r7 := start.AddDate(0, 0, 5).Unix()
				if m >= 5*1440 {
					r7 = start.AddDate(0, 0, 12).Unix()
				}
				// Utilization rises within a window and drops only at its reset.
				u5 := float64((m+300-phase)%300) / 300
				line, errMarshal := json.Marshal(Sample{
					T:      start.Add(time.Duration(m) * time.Minute),
					AuthID: fmt.Sprintf("claude-%d", i),
					U5:     &u5,
					R5:     &r5,
					R7:     &r7,
				})
				if errMarshal != nil {
					tb.Fatal(errMarshal)
				}
				_, _ = w.Write(append(line, '\n'))
			}
		}
		if errFlush := w.Flush(); errFlush != nil {
			tb.Fatal(errFlush)
		}
		if errClose := f.Close(); errClose != nil {
			tb.Fatal(errClose)
		}
	}
	return start
}

func TestQueryStepReducesGeneratedHistory(t *testing.T) {
	st := NewStore(t.TempDir())
	start := writeGeneratedHistory(t, st.dir)
	until := start.AddDate(0, 0, genDays)

	raw, truncated, errQuery := st.Query(start, until, nil, 0, MaxQuerySamples)
	if errQuery != nil || truncated || len(raw) != genDays*1440*genAuths {
		t.Fatalf("raw = %d truncated=%v err=%v", len(raw), truncated, errQuery)
	}

	reduced, truncated, errQuery := st.Query(start, until, nil, 900, MaxQuerySamples)
	if errQuery != nil || truncated {
		t.Fatalf("truncated=%v err=%v", truncated, errQuery)
	}
	buckets := genDays * 96 * genAuths
	resets5h := genAuths * (genDays * 1440 / 300) * 2 // both neighbours, never a bucket's last sample
	resets7d := genAuths * 1                          // the previous sample already ends its bucket
	firsts := genAuths                                // minute 0 is never a bucket's last sample
	if want := buckets + resets5h + resets7d + firsts; len(reduced) != want {
		t.Fatalf("step=900 samples = %d, want %d", len(reduced), want)
	}
	if !reflect.DeepEqual(reduced, referenceReduce(raw, 900)) {
		t.Fatal("step=900 output differs from the reference reduction")
	}
	encoded, _ := json.Marshal(reduced)
	t.Logf("raw=%d step900=%d (%d bytes JSON)", len(raw), len(reduced), len(encoded))

	// Trimming after each day file gives the same result as trimming once.
	for _, limit := range []int{1, 1000} {
		capped, truncated, errQuery := st.Query(start, until, nil, 900, limit)
		want := reduced[len(reduced)-limit:]
		if errQuery != nil || !truncated || !reflect.DeepEqual(capped, want) {
			t.Fatalf("limit %d: %d samples truncated=%v err=%v", limit, len(capped), truncated, errQuery)
		}
	}
	rawCapped, truncated, _ := st.Query(start, until, nil, 0, 1000)
	if !truncated || !reflect.DeepEqual(rawCapped, raw[len(raw)-1000:]) {
		t.Fatalf("raw cap: %d samples truncated=%v", len(rawCapped), truncated)
	}

	// A range starting mid-day and a subset of auths match the reference too.
	since := start.Add(36*time.Hour + 7*time.Minute)
	ids := []string{"claude-1", "claude-4"}
	subset, _, _ := st.Query(since, until, ids, 0, 0)
	got, _, _ := st.Query(since, until, ids, 3600, 0)
	if len(got) == 0 || !reflect.DeepEqual(got, referenceReduce(subset, 3600)) {
		t.Fatalf("subset reduction mismatch: %d samples", len(got))
	}
}

func BenchmarkQueryGeneratedHistory(b *testing.B) {
	st := NewStore(b.TempDir())
	start := writeGeneratedHistory(b, st.dir)
	until := start.AddDate(0, 0, genDays)
	for _, step := range []int64{0, 900} {
		b.Run(fmt.Sprintf("step=%d", step), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, errQuery := st.Query(start, until, nil, step, MaxQuerySamples); errQuery != nil {
					b.Fatal(errQuery)
				}
			}
		})
	}
}

func TestSameResetToleratesMissingAndNearValues(t *testing.T) {
	v := func(n int64) *int64 { return &n }
	cases := []struct {
		name string
		a, b *int64
		want bool
	}{
		{"both missing", nil, nil, true},
		{"one missing", v(100), nil, true},
		{"other missing", nil, v(100), true},
		{"equal", v(100), v(100), true},
		{"one second apart", v(100), v(101), true},
		{"within tolerance", v(100), v(160), true},
		{"beyond tolerance", v(100), v(161), false},
		{"new window", v(100), v(18100), false},
	}
	for _, tc := range cases {
		if got := sameReset(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: sameReset = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestWindowChangedOnUtilizationDrop(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	x := int64(18000)
	y := int64(36000)
	cases := []struct {
		name string
		prev Sample
		cur  Sample
		want bool
	}{
		{"same window rising", Sample{U5: f(0.3), R5: &x}, Sample{U5: f(0.9), R5: &x}, false},
		{"reset moved", Sample{U5: f(0.9), R5: &x}, Sample{U5: f(0.05), R5: &y}, true},
		{"drop without resets", Sample{U5: f(0.9)}, Sample{U5: f(0)}, true},
		{"drop with one-sided reset", Sample{U5: f(0.9), R5: &x}, Sample{U5: f(0)}, true},
		{"weekly drop", Sample{U7: f(0.5), R7: &x}, Sample{U7: f(0.1), R7: &x}, true},
		{"header then rounded usage percentage", Sample{U5: f(0.1234), R5: &x}, Sample{U5: f(12.3 / 100), R5: &x}, false},
		{"weekly rounding without resets", Sample{U7: f(0.4567)}, Sample{U7: f(45.6 / 100)}, false},
		{"drop beyond tolerance", Sample{U5: f(0.5)}, Sample{U5: f(0.498)}, true},
		{"missing utilization", Sample{R5: &x}, Sample{U5: f(0.2), R5: &x}, false},
	}
	for _, tc := range cases {
		if got := windowChanged(tc.prev, tc.cur); got != tc.want {
			t.Errorf("%s: windowChanged = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestQueryStepKeepsBoundaryWhenResetMissing(t *testing.T) {
	st := NewStore(t.TempDir())
	base := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	f := func(v float64) *float64 { return &v }
	appendSamples(t, st,
		Sample{T: at(0), AuthID: "a", U5: f(0.3), R5: i64(18000)},
		Sample{T: at(5), AuthID: "a", U5: f(0.9), R5: i64(18000)},
		Sample{T: at(10), AuthID: "a", U5: f(0)},
		Sample{T: at(15), AuthID: "a", U5: f(0.05), R5: i64(36000)},
	)
	samples, _, errQuery := st.Query(base, base.Add(time.Hour), nil, 3600, 0)
	if errQuery != nil {
		t.Fatal(errQuery)
	}
	// The final reading of the first window (0.9) and the first of the next are kept.
	assertSamples(t, samples, "a@2026-10-27T10:00:00", "a@2026-10-27T10:05:00",
		"a@2026-10-27T10:10:00", "a@2026-10-27T10:15:00")
}

func TestRecorderRecordsFirstWeeklyReadingAmongFiveHourSamples(t *testing.T) {
	r := NewRecorder(NewStore(t.TempDir()))
	auth := &coreauth.Auth{ID: "claude-a", Provider: "claude"}
	base := time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)
	fiveHourOnly := func(u string, at time.Duration) {
		h := http.Header{}
		h.Set("Anthropic-Ratelimit-Unified-5h-Utilization", u)
		h.Set("Anthropic-Ratelimit-Unified-5h-Reset", "1761580800")
		r.Record(auth, h, base.Add(at))
	}
	fiveHourOnly("0.10", 0)
	fiveHourOnly("0.11", 5*time.Second)
	// A usage refresh brings the first 7d reading 10s later: recorded despite the 5h throttle.
	usage := Sample{U5: float(0.11), R5: i64(1761580800), U7: float(0.5), R7: i64(1762000000)}
	r.RecordSample(auth, usage, base.Add(10*time.Second))
	got := drain(r)
	if len(got) != 2 || got[1].U7 == nil || *got[1].U7 != 0.5 {
		t.Fatalf("samples = %+v", got)
	}
	// The same reading again within the minute is throttled; 5h-only samples stay throttled too.
	r.RecordSample(auth, usage, base.Add(20*time.Second))
	fiveHourOnly("0.12", 30*time.Second)
	if got := drain(r); len(got) != 0 {
		t.Fatalf("throttled = %+v", got)
	}
	// A 7d utilization drop (new weekly window) bypasses the throttle.
	next := usage
	next.U7 = float(0.01)
	r.RecordSample(auth, next, base.Add(40*time.Second))
	if got := drain(r); len(got) != 1 || *got[0].U7 != 0.01 {
		t.Fatalf("weekly change = %+v", got)
	}
}

func TestQueryStepKeepsWeeklyReadingsAmongFiveHourSamples(t *testing.T) {
	st := NewStore(t.TempDir())
	base := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	r5 := i64(base.Add(5 * time.Hour).Unix())
	r7 := i64(base.Add(5 * 24 * time.Hour).Unix())
	weekly := 0
	for m := 0; m < 120; m++ {
		s := Sample{T: base.Add(time.Duration(m) * time.Minute), AuthID: "a", U5: float(float64(m) / 200), R5: r5}
		if m%15 == 7 {
			s.U7 = float(0.3 + float64(m)/1000)
			s.R7 = r7
			weekly++
		}
		appendSamples(t, st, s)
	}
	samples, _, errQuery := st.Query(base, base.Add(2*time.Hour), nil, 900, 0)
	if errQuery != nil {
		t.Fatal(errQuery)
	}
	got := 0
	for _, s := range samples {
		if s.U7 != nil {
			got++
		}
	}
	if got != weekly || weekly != 8 {
		t.Fatalf("weekly readings kept = %d of %d in %v", got, weekly, sampleTimes(samples))
	}
	// Still at most the last 5h sample per bucket plus the first, so the reduction holds.
	if len(samples) > 8+8+1 {
		t.Fatalf("too many samples: %d", len(samples))
	}
}

func TestQueryStepKeepsWeeklyBoundaryAcrossFiveHourOnlySamples(t *testing.T) {
	st := NewStore(t.TempDir())
	base := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	r5 := i64(18000)
	w1, w2 := i64(604800), i64(1209600)
	appendSamples(t, st,
		Sample{T: at(0), AuthID: "a", U5: float(0.1), R5: r5, U7: float(0.80), R7: w1},
		Sample{T: at(5), AuthID: "a", U5: float(0.2), R5: r5, U7: float(0.95), R7: w1},
		Sample{T: at(10), AuthID: "a", U5: float(0.3), R5: r5},
		Sample{T: at(15), AuthID: "a", U5: float(0.4), R5: r5, U7: float(0.10), R7: w2},
		Sample{T: at(20), AuthID: "a", U5: float(0.5), R5: r5, U7: float(0.20), R7: w2},
	)
	samples, _, errQuery := st.Query(base, base.Add(time.Hour), nil, 3600, 0)
	if errQuery != nil {
		t.Fatal(errQuery)
	}
	// Both neighbours of the weekly change (0.95 and 0.10) survive the 5h-only sample between them.
	assertSamples(t, samples, "a@2026-10-27T10:00:00", "a@2026-10-27T10:05:00",
		"a@2026-10-27T10:15:00", "a@2026-10-27T10:20:00")
}
