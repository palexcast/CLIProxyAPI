package registry

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// catalogRetryInitial is the first wait after a failed catalog refresh; each
// further failure doubles it up to ModelsRefreshInterval.
const catalogRetryInitial = 30 * time.Second

var catalogCache struct {
	sync.Mutex
	dir string
}

// SetModelCatalogCacheDir sets where fetched catalogs are saved and read back
// on start, so a restart without network keeps the models added since the
// embedded catalog was built. An empty dir, the default, disables the cache.
func SetModelCatalogCacheDir(dir string) {
	catalogCache.Lock()
	defer catalogCache.Unlock()
	catalogCache.dir = dir
}

// catalogCachePath keys the copy by source, so switching catalog source never
// serves a catalog saved from another one.
func catalogCachePath(name, source string) string {
	catalogCache.Lock()
	dir := catalogCache.dir
	catalogCache.Unlock()
	if dir == "" || name == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(source))
	return filepath.Join(dir, fmt.Sprintf("%s-%x.json", name, sum[:6]))
}

func readCatalogCache(name, source string) []byte {
	path := catalogCachePath(name, source)
	if path == "" {
		return nil
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		if !os.IsNotExist(errRead) {
			log.WithError(errRead).Warn("saved model catalog unreadable")
		}
		return nil
	}
	return data
}

func writeCatalogCache(name, source string, data []byte) {
	path := catalogCachePath(name, source)
	if path == "" {
		return
	}
	if errWrite := writeFileAtomic(path, data); errWrite != nil {
		log.WithError(errWrite).Warn("failed to save model catalog")
	}
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return errMkdir
	}
	tmp, errCreate := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if errCreate != nil {
		return errCreate
	}
	tmpName := tmp.Name()
	if _, errWrite := tmp.Write(data); errWrite != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return errWrite
	}
	if errClose := tmp.Close(); errClose != nil {
		_ = os.Remove(tmpName)
		return errClose
	}
	if errRename := os.Rename(tmpName, path); errRename != nil {
		_ = os.Remove(tmpName)
		return errRename
	}
	return nil
}

func nextCatalogRetry(current time.Duration) time.Duration {
	next := current * 2
	if next > ModelsRefreshInterval {
		return ModelsRefreshInterval
	}
	return next
}
