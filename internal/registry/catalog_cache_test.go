package registry

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func useCatalogCacheDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	SetModelCatalogCacheDir(dir)
	t.Cleanup(func() { SetModelCatalogCacheDir("") })
	return dir
}

func waitPublished(t *testing.T, published <-chan string, want string) {
	t.Helper()
	select {
	case got := <-published:
		if got != want {
			t.Fatalf("published %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%q was not published", want)
	}
}

// waitSaved polls because the copy is written after the publish it follows.
func waitSaved(t *testing.T, source, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for string(readCatalogCache("models", source)) != want {
		if time.Now().After(deadline) {
			t.Fatalf("saved copy is %q, want %q", readCatalogCache("models", source), want)
		}
		runtime.Gosched()
	}
}

func recordingUpdater(fetch func(context.Context, string) ([]byte, error)) (*catalogUpdater, chan string) {
	published := make(chan string, 8)
	return &catalogUpdater{
		cacheName: "models",
		fetch:     fetch,
		publish:   func(data []byte) ([]string, error) { published <- string(data); return nil, nil },
	}, published
}

func TestCatalogSavedCopyServesRestartWithoutNetwork(t *testing.T) {
	useCatalogCacheDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	online, published := recordingUpdater(func(context.Context, string) ([]byte, error) { return []byte("fetched"), nil })
	if !online.refresh(ctx, "https://catalog.example", 0) {
		t.Fatal("refresh failed")
	}
	waitPublished(t, published, "fetched")

	offline, published := recordingUpdater(func(context.Context, string) ([]byte, error) { return nil, errors.New("no such host") })
	offline.configure(ctx, "https://catalog.example")
	waitPublished(t, published, "fetched")
}

func TestCatalogSavedCopyIsKeyedBySource(t *testing.T) {
	useCatalogCacheDir(t)
	if readCatalogCache("models", "a") != nil {
		t.Fatal("empty cache returned data")
	}
	writeCatalogCache("models", "a", []byte("from a"))
	if got := readCatalogCache("models", "b"); got != nil {
		t.Fatalf("source b read %q saved for source a", got)
	}
	if got := readCatalogCache("codex-client-models", "a"); got != nil {
		t.Fatalf("another catalog read %q", got)
	}
	if got := string(readCatalogCache("models", "a")); got != "from a" {
		t.Fatalf("read %q", got)
	}
}

func TestCatalogFetchReplacesSavedCopy(t *testing.T) {
	useCatalogCacheDir(t)
	writeCatalogCache("models", "src", []byte("saved"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u, published := recordingUpdater(func(context.Context, string) ([]byte, error) { return []byte("fetched"), nil })
	u.configure(ctx, "src")
	waitPublished(t, published, "saved")
	waitPublished(t, published, "fetched")
	waitSaved(t, "src", "fetched")
}

func TestCatalogRejectedSavedCopyStillFetches(t *testing.T) {
	useCatalogCacheDir(t)
	writeCatalogCache("models", "src", []byte("corrupt"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	published := make(chan string, 4)
	u := &catalogUpdater{
		cacheName: "models",
		fetch:     func(context.Context, string) ([]byte, error) { return []byte("fetched"), nil },
		publish: func(data []byte) ([]string, error) {
			if string(data) == "corrupt" {
				return nil, errors.New("invalid catalog")
			}
			published <- string(data)
			return nil, nil
		},
	}
	u.configure(ctx, "src")
	waitPublished(t, published, "fetched")
	waitSaved(t, "src", "fetched")
}

func TestCatalogEmbeddedSourceIsNotSaved(t *testing.T) {
	dir := useCatalogCacheDir(t)
	u, published := recordingUpdater(func(context.Context, string) ([]byte, error) { return []byte("embedded"), nil })
	u.refresh(context.Background(), embeddedCatalogSource, 0)
	waitPublished(t, published, "embedded")
	entries, errRead := os.ReadDir(dir)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if len(entries) != 0 {
		t.Fatalf("embedded catalog saved %d files", len(entries))
	}
}

func TestCatalogOnDemandRefreshSavesCopy(t *testing.T) {
	useCatalogCacheDir(t)
	u, published := recordingUpdater(func(context.Context, string) ([]byte, error) { return []byte("on demand"), nil })
	if _, errRefresh := u.refreshOnDemand(context.Background(), "src", nil); errRefresh != nil {
		t.Fatal(errRefresh)
	}
	waitPublished(t, published, "on demand")
	waitSaved(t, "src", "on demand")
}

func TestCatalogFailedRefreshRetriesBeforeInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	u, published := recordingUpdater(func(context.Context, string) ([]byte, error) {
		if attempts.Add(1) < 3 {
			return nil, errors.New("no such host")
		}
		return []byte("fetched"), nil
	})
	u.retryInitial = time.Millisecond
	u.configure(ctx, "src")
	waitPublished(t, published, "fetched")
	if got := attempts.Load(); got != 3 {
		t.Fatalf("fetched %d times", got)
	}
}

func TestNextCatalogRetryDoublesUpToRefreshInterval(t *testing.T) {
	if got := nextCatalogRetry(30 * time.Second); got != time.Minute {
		t.Fatalf("got %v", got)
	}
	if got := nextCatalogRetry(ModelsRefreshInterval - time.Minute); got != ModelsRefreshInterval {
		t.Fatalf("got %v", got)
	}
}
