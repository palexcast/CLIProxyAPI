package quotahistory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// SnapshotFileName is the snapshot file kept next to the day files. It must
	// not end in .json: the file token store loads every *.json file under the
	// auth directory as a credential.
	SnapshotFileName = "snapshot.state"
	// MaxUsageBytes caps a stored usage-endpoint body.
	MaxUsageBytes = 64 << 10

	snapshotDebounce = 5 * time.Second
	seedDays         = 7
)

// SnapshotEntry is the latest known quota state of one account: the newest
// reading of each rate-limit window and the last usage-endpoint body.
type SnapshotEntry struct {
	ObservedAt  *time.Time      `json:"observed_at,omitempty"`
	U5          *float64        `json:"u5,omitempty"`
	R5          *int64          `json:"r5,omitempty"`
	S5          string          `json:"s5,omitempty"`
	U7          *float64        `json:"u7,omitempty"`
	R7          *int64          `json:"r7,omitempty"`
	S7          string          `json:"s7,omitempty"`
	RefreshedAt *time.Time      `json:"refreshed_at,omitempty"`
	Usage       json.RawMessage `json:"usage,omitempty"`
}

type snapshotFile struct {
	Accounts map[string]SnapshotEntry `json:"accounts"`
}

// Snapshot keeps the latest quota state per auth in memory and persists it to
// one JSON file. Entries are replaced, never mutated in place, so a copy of
// the map is safe to read without the lock.
type Snapshot struct {
	dir string

	mu        sync.Mutex
	accounts  map[string]SnapshotEntry
	dirty     bool
	lastWrite time.Time
}

// NewSnapshot returns an empty snapshot persisted under dir.
func NewSnapshot(dir string) *Snapshot {
	return &Snapshot{dir: dir, accounts: make(map[string]SnapshotEntry)}
}

func (sn *Snapshot) path() string {
	return filepath.Join(sn.dir, SnapshotFileName)
}

// Accounts returns a copy of the entries keyed by auth id.
func (sn *Snapshot) Accounts() map[string]SnapshotEntry {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	out := make(map[string]SnapshotEntry, len(sn.accounts))
	for id, e := range sn.accounts {
		out[id] = e
	}
	return out
}

// ApplySample folds a sample into its auth's entry. A sample older than the
// entry's observed_at is ignored. Each window is taken from the sample only
// when the sample carries that window's utilization; a window change clears
// the older reset and status before the sample's own values are applied. A
// window the sample does not carry is dropped once its reset is at or before
// the sample: a response after the reset that reports no such window means the
// account has none now (a verified Ultra plan has no 5h limit), and the old
// reading describes a window that is over.
func (sn *Snapshot) ApplySample(s Sample) {
	if s.AuthID == "" || s.T.IsZero() || (s.U5 == nil && s.U7 == nil) {
		return
	}
	sn.mu.Lock()
	defer sn.mu.Unlock()
	e := sn.accounts[s.AuthID]
	if e.ObservedAt != nil && s.T.Before(*e.ObservedAt) {
		return
	}
	at := s.T.UTC()
	e.ObservedAt = &at
	e.U5, e.R5, e.S5 = applyWindow(e.U5, e.R5, e.S5, s.U5, s.R5, s.S5)
	e.U7, e.R7, e.S7 = applyWindow(e.U7, e.R7, e.S7, s.U7, s.R7, s.S7)
	e.dropEndedWindows()
	sn.accounts[s.AuthID] = e
	sn.dirty = true
}

// dropEndedWindows clears every window whose reset is at or before observed_at.
// A reading taken after its own reset would carry the next reset, so such a
// window was absent from every sample since it ended.
func (e *SnapshotEntry) dropEndedWindows() {
	if e.ObservedAt == nil {
		return
	}
	at := e.ObservedAt.Unix()
	if e.R5 != nil && *e.R5 <= at {
		e.U5, e.R5, e.S5 = nil, nil, ""
	}
	if e.R7 != nil && *e.R7 <= at {
		e.U7, e.R7, e.S7 = nil, nil, ""
	}
}

func applyWindow(u *float64, r *int64, status string, nu *float64, nr *int64, nstatus string) (*float64, *int64, string) {
	if nu == nil {
		return u, r, status
	}
	if !sameReset(r, nr) || utilizationDropped(u, nu) {
		r, status = nil, ""
	}
	if nr != nil {
		r = nr
	}
	if nstatus != "" {
		status = nstatus
	}
	return nu, r, status
}

// ApplyUsage stores a usage-endpoint body for an auth unless a newer one is
// already stored. body must already be a compact JSON object.
func (sn *Snapshot) ApplyUsage(authID string, body json.RawMessage, at time.Time) {
	if authID == "" || len(body) == 0 || at.IsZero() {
		return
	}
	sn.mu.Lock()
	defer sn.mu.Unlock()
	e := sn.accounts[authID]
	if e.RefreshedAt != nil && at.Before(*e.RefreshedAt) {
		return
	}
	at = at.UTC()
	e.RefreshedAt = &at
	e.Usage = body
	sn.accounts[authID] = e
	sn.dirty = true
}

// normalizeUsage validates a usage-endpoint body as a JSON object of at most
// MaxUsageBytes and returns it compacted.
func normalizeUsage(body []byte) (json.RawMessage, bool) {
	if len(body) == 0 || len(body) > MaxUsageBytes {
		return nil, false
	}
	var object map[string]json.RawMessage
	if errDecode := json.Unmarshal(body, &object); errDecode != nil || object == nil {
		return nil, false
	}
	var buf bytes.Buffer
	if errCompact := json.Compact(&buf, body); errCompact != nil {
		return nil, false
	}
	return json.RawMessage(buf.Bytes()), true
}

// Load replaces the entries with the snapshot file. It reports false when the
// file does not exist.
func (sn *Snapshot) Load() (bool, error) {
	data, errRead := os.ReadFile(sn.path())
	if errRead != nil {
		if os.IsNotExist(errRead) {
			return false, nil
		}
		return false, fmt.Errorf("read quota snapshot: %w", errRead)
	}
	var file snapshotFile
	if errDecode := json.Unmarshal(data, &file); errDecode != nil {
		return false, fmt.Errorf("decode quota snapshot: %w", errDecode)
	}
	accounts := make(map[string]SnapshotEntry, len(file.Accounts))
	for id, e := range file.Accounts {
		if id != "" {
			// A file written before ended windows were dropped may still hold one.
			e.dropEndedWindows()
			accounts[id] = e
		}
	}
	sn.mu.Lock()
	sn.accounts = accounts
	sn.dirty = false
	sn.mu.Unlock()
	return true, nil
}

// Seed applies the samples of the day files covering the seedDays days up to
// now, oldest first, so each window ends at its newest reading.
func (sn *Snapshot) Seed(st *Store, now time.Time) error {
	today := now.UTC().Truncate(24 * time.Hour)
	var day []Sample
	for i := seedDays - 1; i >= 0; i-- {
		day = day[:0]
		if errRead := st.readDay(today.AddDate(0, 0, -i), func(s Sample) { day = append(day, s) }); errRead != nil {
			return errRead
		}
		sortSamples(day)
		for _, s := range day {
			sn.ApplySample(s)
		}
	}
	return nil
}

// Flush writes the snapshot when it changed. Unless force is set it writes at
// most once per snapshotDebounce, measured from the previous write.
func (sn *Snapshot) Flush(now time.Time, force bool) error {
	sn.mu.Lock()
	if !sn.dirty || (!force && !sn.lastWrite.IsZero() && now.Sub(sn.lastWrite) < snapshotDebounce) {
		sn.mu.Unlock()
		return nil
	}
	data, errMarshal := json.Marshal(snapshotFile{Accounts: sn.accounts})
	sn.dirty = false
	sn.lastWrite = now
	sn.mu.Unlock()

	errWrite := errMarshal
	if errWrite == nil {
		errWrite = sn.write(data)
	}
	if errWrite != nil {
		sn.mu.Lock()
		sn.dirty = true
		sn.mu.Unlock()
		return fmt.Errorf("write quota snapshot: %w", errWrite)
	}
	return nil
}

// write replaces the snapshot file atomically through a temporary file in the
// same directory.
func (sn *Snapshot) write(data []byte) error {
	if errMkdir := os.MkdirAll(sn.dir, 0o700); errMkdir != nil {
		return errMkdir
	}
	tmp, errCreate := os.CreateTemp(sn.dir, SnapshotFileName+".*.tmp")
	if errCreate != nil {
		return errCreate
	}
	tmpPath := tmp.Name()
	_, errWrite := tmp.Write(data)
	if errWrite == nil {
		// Without a sync the rename can reach the disk before the data, and a
		// power loss would leave an empty or partial snapshot.
		errWrite = tmp.Sync()
	}
	if errClose := tmp.Close(); errWrite == nil {
		errWrite = errClose
	}
	if errWrite == nil {
		errWrite = os.Chmod(tmpPath, 0o600)
	}
	if errWrite == nil {
		errWrite = os.Rename(tmpPath, sn.path())
	}
	if errWrite != nil {
		if errRemove := os.Remove(tmpPath); errRemove != nil && !os.IsNotExist(errRemove) {
			log.Warnf("quota history: remove snapshot temp file: %v", errRemove)
		}
		return errWrite
	}
	return nil
}
