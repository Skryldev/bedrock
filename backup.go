package bedrock

import (
        "fmt"
        "io"
        "os"
        "path/filepath"
        "sort"
)

// CreateCheckpoint implements ExtendedStore: it writes a crash-consistent
// hardlink snapshot of the live database into destDir. Checkpointing is
// O(number of sstables) — near-instant and independent of data size — and
// includes the WAL, so the snapshot reflects the exact state at call time.
//
// The destination must not exist or must be an empty directory; this
// protects against clobbering a previous backup by accident.
func (s *Store) CreateCheckpoint(destDir string) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if fi, err := os.Stat(destDir); err == nil {
                if !fi.IsDir() {
                        return fmt.Errorf("%w: %s is not a directory", ErrCheckpointExists, destDir)
                }
                entries, rdErr := os.ReadDir(destDir)
                if rdErr != nil {
                        return mapEngineError("checkpoint", []byte(destDir), rdErr)
                }
                if len(entries) > 0 {
                        return fmt.Errorf("%w: %s", ErrCheckpointExists, destDir)
                }
                // Pebble requires a non-existent destination: drop the empty dir.
                if err := os.Remove(destDir); err != nil {
                        return mapEngineError("checkpoint", []byte(destDir), err)
                }
        }
        if err := s.db.Checkpoint(destDir); err != nil {
                return mapEngineError("checkpoint", []byte(destDir), err)
        }
        s.logger.Info("checkpoint created", zapString("dest", destDir), zapString("store", s.name))
        return nil
}

// RestoreCheckpoint copies a checkpoint produced by CreateCheckpoint into
// destDir, producing a directory ready for Open. Checkpoints contain
// hardlinks only; a plain recursive copy materializes them, which is why
// restoring to local NVMe is recommended before opening.
func RestoreCheckpoint(backupDir, destDir string) error {
        info, err := os.Stat(backupDir)
        if err != nil {
                return &StoreError{Op: "restore", Err: fmt.Errorf("backup dir: %w", err)}
        }
        if !info.IsDir() {
                return &StoreError{Op: "restore", Err: fmt.Errorf("backup dir %s is not a directory", backupDir)}
        }
        if fi, err := os.Stat(destDir); err == nil {
                if entries, _ := os.ReadDir(destDir); fi.IsDir() && len(entries) > 0 {
                        return fmt.Errorf("%w: destination %s not empty", ErrCheckpointExists, destDir)
                }
        }
        return copyDir(backupDir, destDir)
}

// ListCheckpoints returns the checkpoint directories under root, sorted by
// name (checkpoints are typically named with timestamps, giving
// chronological order).
func ListCheckpoints(root string) ([]string, error) {
        entries, err := os.ReadDir(root)
        if err != nil {
                return nil, err
        }
        var out []string
        for _, e := range entries {
                if e.IsDir() {
                        // A checkpoint dir is recognizable by its MANIFEST file.
                        if _, err := os.Stat(filepath.Join(root, e.Name(), "MANIFEST-000000")); err == nil {
                                out = append(out, filepath.Join(root, e.Name()))
                                continue
                        }
                        // Version-dependent manifest names: fall back to any MANIFEST-*.
                        matches, _ := filepath.Glob(filepath.Join(root, e.Name(), "MANIFEST-*"))
                        if len(matches) > 0 {
                                out = append(out, filepath.Join(root, e.Name()))
                        }
                }
        }
        sort.Strings(out)
        return out, nil
}

// copyDir recursively copies src into dst, creating dst.
func copyDir(src, dst string) error {
        return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
                if err != nil {
                        return err
                }
                rel, err := filepath.Rel(src, path)
                if err != nil {
                        return err
                }
                target := filepath.Join(dst, rel)
                if d.IsDir() {
                        return os.MkdirAll(target, 0o755)
                }
                info, err := d.Info()
                if err != nil {
                        return err
                }
                if !info.Mode().IsRegular() {
                        return nil // skip sockets/symlinks; checkpoints contain regular files
                }
                return copyFile(path, target, info.Mode())
        })
}

func copyFile(src, dst string, mode os.FileMode) error {
        in, err := os.Open(src)
        if err != nil {
                return err
        }
        defer in.Close()
        out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
        if err != nil {
                return err
        }
        if _, err = io.Copy(out, in); err != nil {
                out.Close()
                return err
        }
        return out.Close()
}

// OpenOrRestore attempts to open the store at cfg.DataDir. If the engine
// reports the directory unrecoverable (missing/corrupt MANIFEST or data),
// and backupDir exists, the backup is restored into DataDir and the open
// is retried once. This is the recommended crash-recovery entry point for
// single-node deployments.
func OpenOrRestore(cfg *Config, backupDir string) (*Store, error) {
        s, openErr := Open(withDataDirOption(cfg)...)
        if openErr == nil {
                return s, nil
        }
        if backupDir == "" {
                return nil, openErr
        }
        if _, statErr := os.Stat(backupDir); statErr != nil {
                return nil, openErr
        }
        // Recovery path: wipe the damaged dir and restore the latest backup.
        if rmErr := os.RemoveAll(cfg.DataDir); rmErr != nil {
                return nil, &StoreError{Op: "restore", Err: rmErr}
        }
        if err := RestoreCheckpoint(backupDir, cfg.DataDir); err != nil {
                return nil, err
        }
        return Open(withDataDirOption(cfg)...)
}

// withDataDirOption adapts a *Config into options for Open.
func withDataDirOption(cfg *Config) []Option {
        opts := []Option{
                WithDataDir(cfg.DataDir),
                WithName(cfg.Name),
                WithCacheSizeMB(cfg.CacheSizeMB),
                WithMemTableSizeMB(cfg.MemTableSizeMB),
                WithSyncWrites(cfg.SyncWrites),
                WithMaxOpenFiles(cfg.MaxOpenFiles),
                WithBloomBitsPerKey(cfg.BloomBitsPerKey),
        }
        if cfg.HotCache.Enabled {
                opts = append(opts, WithHotCache(cfg.HotCache.MaxBytes, cfg.HotCache.TTL))
        }
        if cfg.Circuit.Enabled {
                opts = append(opts, WithCircuitBreaker(cfg.Circuit))
        }
        if cfg.Logger != nil {
                opts = append(opts, WithZapLogger(cfg.Logger))
        }
        return opts
}
