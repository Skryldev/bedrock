package bedrock

import "context"

// Span abstracts a tracing span without depending on any tracing SDK.
// Bridge adapters (e.g. OpenTelemetry) wrap an otel.Tracer into Tracer and
// otel.Span into Span; the hot path only touches Span when a non-noop
// Tracer is installed.
type Span interface {
	// SetAttr attaches an attribute to the span.
	SetAttr(key, val string)
	// End completes the span. err may be nil.
	End(err error)
}

// Tracer starts operation spans. Implementations must be safe for
// concurrent use and must return quickly — StartSpan sits on every
// operation's hot path.
type Tracer interface {
	StartSpan(ctx context.Context, op string, key []byte) (context.Context, Span)
}

// noopSpan is a shared singleton; zero allocation.
type noopSpan struct{}

func (noopSpan) SetAttr(string, string) {}
func (noopSpan) End(error)              {}

// NoopSpan is the shared no-op span instance.
var NoopSpan Span = noopSpan{}

// noopTracer is the default tracer: it returns the context unchanged and
// the shared NoopSpan, costing a couple of nanoseconds per operation.
type noopTracer struct{}

func (noopTracer) StartSpan(ctx context.Context, _ string, _ []byte) (context.Context, Span) {
	return ctx, NoopSpan
}

// NoopTracer is the default tracer instance.
var NoopTracer Tracer = noopTracer{}

// spanFromCtx retrieves the span previously installed by StartSpan via
// traceCtxKey. Used internally to annotate child activity.
type traceCtxKey struct{}

func ctxWithSpan(ctx context.Context, sp Span) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, sp)
}

// SpanFromContext returns the active store span, or NoopSpan.
func SpanFromContext(ctx context.Context) Span {
	if sp, ok := ctx.Value(traceCtxKey{}).(Span); ok && sp != nil {
		return sp
	}
	return NoopSpan
}
