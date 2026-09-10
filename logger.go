package bedrock

import (
        "context"
        "time"

        "go.uber.org/zap"
        "go.uber.org/zap/zapcore"
)

// CtxFieldExtractor derives log fields from an operation context. Install
// via WithContextFields to propagate request IDs / trace IDs from context
// into every structured log line emitted by the store.
type CtxFieldExtractor func(ctx context.Context) []zap.Field

// defaultLogger returns a nop logger so zero-config usage never pays for
// logging.
func defaultLogger() *zap.Logger { return zap.NewNop() }

// newProductionZap is a convenience constructor exposed for users who want
// a ready-made JSON logger tuned for this module.
func newProductionZap() *zap.Logger {
        cfg := zap.NewProductionConfig()
        cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
        l, _ := cfg.Build()
        return l
}

// withCtx augments a log entry with context-extracted fields.
func withCtx(ctx context.Context, logger *zap.Logger, extractor CtxFieldExtractor) *zap.Logger {
        if ctx == nil || extractor == nil {
                return logger
        }
        if fields := extractor(ctx); len(fields) > 0 {
                return logger.With(fields...)
        }
        return logger
}

// field helpers keep call sites tidy.
func zapErr(err error) zap.Field { return zap.NamedError("error", err) }

func zapKey(key []byte) zap.Field { return zap.ByteString("key", key) }

func zapOp(op string) zap.Field { return zap.String("op", op) }

func zapDur(d time.Duration) zap.Field { return zap.Duration("duration", d) }

func zapString(key, val string) zap.Field { return zap.String(key, val) }
