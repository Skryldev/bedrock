package bedrock

import (
        "context"
        "fmt"
        "path/filepath"
        "strings"
        "sync"
        "sync/atomic"

        "go.uber.org/zap"
)

// Repository is a factory and registry for named, isolated store
// instances. Each name maps to one open Pebble directory under BaseDir;
// repeated Open calls for the same name return the same store with a
// bumped reference count, so multi-tenant services can share engines
// safely.
//
// Close semantics are reference-counted: Close(name) releases one
// reference; the underlying engine closes at zero. CloseAll shuts down
// every instance regardless of outstanding references (graceful shutdown
// path for the process).
type Repository struct {
        BaseDir string
        logger  *zap.Logger
        opts    []Option

        mu     sync.RWMutex
        stores map[string]*managedStore
        closed atomic.Bool
}

type managedStore struct {
        store *Store
        refs  int32
}

// NewRepository creates a registry rooted at baseDir. Every store opened
// through it gets DataDir = baseDir/<name> unless its options override the
// data dir explicitly.
func NewRepository(baseDir string, opts ...Option) *Repository {
        logger := defaultLogger()
        cfg := DefaultConfig("default")
        for _, o := range opts {
                o(cfg)
        }
        if cfg.Logger != nil {
                logger = cfg.Logger
        }
        return &Repository{
                BaseDir: baseDir,
                logger:  logger,
                opts:    opts,
                stores:  make(map[string]*managedStore),
        }
}

// validateName rejects path separators and traversal attempts: store names
// become directory names.
func validateName(name string) error {
        if name == "" {
                return ErrInvalidConfig
        }
        if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
                return fmt.Errorf("%w: store name %q must not contain path separators", ErrInvalidConfig, name)
        }
        return nil
}

// Open returns the named store, creating and opening the engine on first
// use. Subsequent calls return the same *Store and increment the
// reference count; pair every Open with exactly one Repository.Close.
//
// Directory layout: every store lives at BaseDir/<name> for strict
// isolation. A name-specific WithDataDir option overrides that placement;
// repository-level WithDataDir options are ignored for layout purposes.
func (r *Repository) Open(name string, opts ...Option) (*Store, error) {
        if r.closed.Load() {
                return nil, ErrRepositoryClosing
        }
        if err := validateName(name); err != nil {
                return nil, err
        }

        r.mu.Lock()
        defer r.mu.Unlock()
        if r.closed.Load() {
                return nil, ErrRepositoryClosing
        }
        if m, ok := r.stores[name]; ok {
                m.refs++
                return m.store, nil
        }

        merged := make([]Option, 0, len(r.opts)+len(opts)+1)
        merged = append(merged, r.opts...)
        merged = append(merged, opts...)
        merged = append(merged, func(c *Config) { c.Name = name })

        // Explicit per-name data dir detection: apply each name-specific
        // option to a probe config; a changed DataDir wins over the default
        // BaseDir/<name> layout.
        dirExplicit := false
        for _, o := range opts {
                probe := DefaultConfig("probe")
                o(probe)
                if probe.DataDir != DefaultConfig("probe").DataDir {
                        dirExplicit = true
                        break
                }
        }
        if !dirExplicit {
                merged = append(merged, WithDataDir(filepath.Join(r.BaseDir, name)))
        }

        s, err := Open(merged...)
        if err != nil {
                return nil, err
        }
        r.stores[name] = &managedStore{store: s, refs: 1}
        r.logger.Info("repository opened store", zap.String("store", name), zap.String("data_dir", s.DataDir()))
        return s, nil
}

// Get returns the named store without affecting reference counts.
func (r *Repository) Get(name string) (*Store, bool) {
        r.mu.RLock()
        defer r.mu.RUnlock()
        m, ok := r.stores[name]
        if !ok {
                return nil, false
        }
        return m.store, true
}

// Close releases one reference to the named store. The engine closes when
// the count reaches zero. Returns ErrStoreNotFound for unknown names.
func (r *Repository) Close(name string) error {
        r.mu.Lock()
        defer r.mu.Unlock()
        m, ok := r.stores[name]
        if !ok {
                return ErrStoreNotFound
        }
        m.refs--
        if m.refs > 0 {
                return nil
        }
        delete(r.stores, name)
        err := m.store.Close()
        if err != nil {
                r.logger.Error("repository close store failed", zap.String("store", name), zapErr(err))
        }
        r.logger.Info("repository closed store", zap.String("store", name))
        return err
}

// Names lists currently open store names.
func (r *Repository) Names() []string {
        r.mu.RLock()
        defer r.mu.RUnlock()
        out := make([]string, 0, len(r.stores))
        for n := range r.stores {
                out = append(out, n)
        }
        return out
}

// CloseAll gracefully shuts down every store: new Opens are rejected,
// each engine drains in-flight operations and closes, and errors are
// aggregated. Safe to call multiple times.
func (r *Repository) CloseAll(ctx context.Context) error {
        r.closed.Store(true)
        r.mu.Lock()
        stores := make([]*managedStore, 0, len(r.stores))
        names := make([]string, 0, len(r.stores))
        for n, m := range r.stores {
                stores = append(stores, m)
                names = append(names, n)
        }
        r.stores = make(map[string]*managedStore)
        r.mu.Unlock()

        var errs []error
        for i, m := range stores {
                if err := m.store.Close(); err != nil {
                        errs = append(errs, fmt.Errorf("store %s: %w", names[i], err))
                }
                r.logger.Info("repository closed store", zap.String("store", names[i]))
        }
        select {
        case <-ctx.Done():
                errs = append(errs, ctx.Err())
        default:
        }
        switch len(errs) {
        case 0:
                return nil
        case 1:
                return errs[0]
        default:
                return &multiCloseError{errs: errs}
        }
}

// multiCloseError aggregates several close failures.
type multiCloseError struct{ errs []error }

func (m *multiCloseError) Error() string {
        parts := make([]string, len(m.errs))
        for i, e := range m.errs {
                parts[i] = e.Error()
        }
        return "bedrock: close errors: " + strings.Join(parts, "; ")
}

func (m *multiCloseError) Unwrap() []error { return m.errs }
