package tmux

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/gastownhall/gascity/internal/runtime"
)

// lockLifecycle serializes same-name runtime identity changes across provider
// instances and processes. The file is a kernel-held coordination lock, not a
// liveness record; it is intentionally never unlinked so contenders lock the
// same inode across session incarnations.
func (p *Provider) lockLifecycle(name string) (*os.File, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("session name is required")
	}
	lockRoot := strings.TrimSpace(p.cfg.RuntimeDir)
	if lockRoot == "" {
		cacheRoot, err := os.UserCacheDir()
		if err != nil {
			return nil, fmt.Errorf("resolving user cache directory: %w", err)
		}
		lockRoot = filepath.Join(cacheRoot, "gascity")
	}
	dir := filepath.Join(lockRoot, "session-lifecycle-locks")
	if err := runtime.EnsurePrivateDir(dir); err != nil {
		return nil, fmt.Errorf("creating lifecycle lock directory: %w", err)
	}
	sum := sha256.Sum256([]byte(p.tm.cfg.SocketName + "\x00" + name))
	path := filepath.Join(dir, hex.EncodeToString(sum[:])+".lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening lifecycle lock for %q: %w", name, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("locking lifecycle for %q: %w", name, err)
	}
	return f, nil
}

func runtimeIdentityMetaKey(key string) bool {
	switch key {
	case "GC_SESSION_ID", "GC_RUNTIME_EPOCH", "GC_INSTANCE_TOKEN":
		return true
	default:
		return false
	}
}
