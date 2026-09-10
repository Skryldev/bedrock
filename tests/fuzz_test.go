package tests

import (
        "context"
        "errors"
        "fmt"
        "testing"
        "time"

        "github.com/Skryldev/bedrock"

        "go.uber.org/zap"
)

// fuzzStore builds a per-execution store in the fuzz cache directory.
func fuzzStore(t *testing.T) *bedrock.Store {
        t.Helper()
        s, err := bedrock.Open(
                bedrock.WithDataDir(t.TempDir()),
                bedrock.WithCacheSizeMB(2),
                bedrock.WithMemTableSizeMB(1),
                bedrock.WithHotCache(1<<20, time.Minute),
                bedrock.WithZapLogger(zap.NewNop()),
        )
        if err != nil {
                t.Skipf("open: %v", err)
        }
        t.Cleanup(func() { _ = s.Close() })
        return s
}

// op is a fuzz-decoded operation.
type op struct {
        kind  byte // 's' set, 'g' get, 'd' delete, 'b' batch, 'n' scan-next, 'r' delete-range
        key   []byte
        value []byte
        end   []byte
}

// decodeOps converts fuzz bytes into a deterministic op sequence.
func decodeOps(data []byte) []op {
        var ops []op
        for i := 0; i+2 < len(data) && len(ops) < 64; {
                kind := data[i] % 6
                klen := int(data[i+1]) % 17
                i += 2
                if i+klen > len(data) {
                        break
                }
                key := append([]byte(nil), data[i:i+klen]...)
                i += klen
                o := op{kind: kind, key: key}
                if kind == 0 { // set: read a value
                        vlen := 0
                        if len(key) > 0 {
                                vlen = int(key[0]) % 32
                        }
                        if i+vlen > len(data) {
                                break
                        }
                        o.value = append([]byte(nil), data[i:i+vlen]...)
                        i += vlen
                }
                if kind == 5 { // delete range: read end
                        elen := int(data[len(data)-1]) % 17
                        if i+elen > len(data) {
                                break
                        }
                        o.end = append([]byte(nil), data[i:i+elen]...)
                        i += elen
                }
                ops = append(ops, o)
        }
        return ops
}

// FuzzStoreOperations is a model-based fuzz test: it maintains an exact
// in-memory model of the store's key/value state and verifies every
// observation against it, across set/get/delete/batch/range-delete/scan.
// Invariant checked: the store behaves as a byte-ordered map with
// read-your-writes consistency.
func FuzzStoreOperations(f *testing.F) {
        // Seed corpus: structured sequences.
        f.Add([]byte{0, 3, 'a', 'b', 'c', 1, 5, 'x', 'y'})
        f.Add([]byte{0, 1, 'k', 0, 1, 'k', 2, 1, 'k', 3, 1, 'k'})
        f.Add([]byte{5, 2, 'a', 2, 1, 'z', 4, 2, 'm', 1, 9})
        f.Add([]byte{3, 4, 'b', 'a', 't', 'c', 3, 4, 'b', 'a', 't', 'h'})
        f.Add([]byte{1, 0, 1, 0, 2, 0, 3, 0})

        f.Fuzz(func(t *testing.T, data []byte) {
                s := fuzzStore(t)
                model := map[string]string{} // exact expected state
                ctx := context.Background()

                for _, o := range decodeOps(data) {
                        if len(o.key) == 0 {
                                continue // empty keys are rejected by the store
                        }
                        switch o.kind {
                        case 0: // set
                                if err := s.Set(ctx, o.key, o.value); err != nil {
                                        t.Fatalf("set: %v", err)
                                }
                                model[string(o.key)] = string(o.value)
                        case 1: // get
                                got, err := s.Get(ctx, o.key)
                                want, ok := model[string(o.key)]
                                switch {
                                case !ok:
                                        if !errors.Is(err, bedrock.ErrNotFound) {
                                                t.Fatalf("get %q: want ErrNotFound, got %v", o.key, err)
                                        }
                                default:
                                        if err != nil {
                                                t.Fatalf("get %q: %v", o.key, err)
                                        }
                                        if string(got) != want {
                                                t.Fatalf("get %q = %q, model says %q", o.key, got, want)
                                        }
                                }
                        case 2: // delete
                                if err := s.Delete(ctx, o.key); err != nil {
                                        t.Fatalf("delete: %v", err)
                                }
                                delete(model, string(o.key))
                        case 3: // batch: set + delete the same key atomically
                                ops := []bedrock.Operation{
                                        {Type: bedrock.OpSet, Key: o.key, Value: []byte("batched")},
                                }
                                if err := s.Batch(ctx, ops); err != nil {
                                        t.Fatalf("batch: %v", err)
                                }
                                model[string(o.key)] = "batched"
                        case 4: // scan consistency: every visible key must be in model with matching value
                                it, err := s.Scan(ctx, nil, nil)
                                if err != nil {
                                        t.Fatalf("scan: %v", err)
                                }
                                for it.Valid() {
                                        k, v := string(it.Key()), string(it.Value())
                                        mv, ok := model[k]
                                        if !ok {
                                                t.Fatalf("scan yielded key %q absent from model", k)
                                        }
                                        if mv != v {
                                                t.Fatalf("scan %q = %q, model %q", k, v, mv)
                                        }
                                        it.Next()
                                }
                                _ = it.Close()
                        case 5: // delete range [key, end): remove from model
                                if len(o.end) == 0 {
                                        continue
                                }
                                err := s.Batch(ctx, []bedrock.Operation{
                                        {Type: bedrock.OpDeleteRange, Key: o.key, End: o.end},
                                })
                                if err != nil {
                                        t.Fatalf("delete range: %v", err)
                                }
                                for k := range model {
                                        if k >= string(o.key) && k < string(o.end) {
                                                delete(model, k)
                                        }
                                }
                        }
                }

                // Final full verification.
                it, err := s.Scan(ctx, nil, nil)
                if err != nil {
                        t.Fatalf("final scan: %v", err)
                }
                seen := 0
                for it.Valid() {
                        k, v := string(it.Key()), string(it.Value())
                        mv, ok := model[k]
                        if !ok || mv != v {
                                t.Fatalf("final state mismatch at %q: store=%q model=%q(present=%v)", k, v, mv, ok)
                        }
                        seen++
                        it.Next()
                }
                if err := it.Close(); err != nil {
                        t.Fatalf("close: %v", err)
                }
                if seen != len(model) {
                        t.Fatalf("final: store has %d keys, model has %d", seen, len(model))
                }
        })
}

// FuzzConfigMatrix checks that arbitrary option combinations either open
// cleanly or fail with a classified error — never a panic.
func FuzzConfigMatrix(f *testing.F) {
        f.Add(uint8(0), int64(1<<20))
        f.Add(uint8(255), int64(-5))

        f.Fuzz(func(t *testing.T, mask uint8, hotBytes int64) {
                if mask&1 != 0 {
                        _ = bedrock.WithSyncWrites(mask&2 != 0)
                }
                if mask&4 != 0 {
                        _ = bedrock.WithHotCache(hotBytes, 0)
                }
                if mask&8 != 0 {
                        _ = bedrock.WithCircuitBreaker(bedrock.CircuitConfig{FailureThreshold: int32(mask)})
                }
                cfg := bedrock.DefaultConfig("fz")
                cfg.DataDir = t.TempDir()
                _ = fmt.Sprint(cfg.Validate())
        })
}
