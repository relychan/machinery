package tracing

import (
	"context"
	"fmt"
	"net/http"

	"github.com/relychan/machinery/v2/tasks"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// default opentelemetry attributes
var (
	MachineryTag     = attribute.String("component", "machinery")
	WorkflowGroupTag = attribute.String("machinery.workflow", "group")
	WorkflowChordTag = attribute.String("machinery.workflow", "chord")
	WorkflowChainTag = attribute.String("machinery.workflow", "chain")
)

var tracer = otel.Tracer("github.com/relychan/machinery/v2")

// StartSpan starts a new span with the given operation name.
func StartSpan(ctx context.Context, operationName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, operationName, trace.WithSpanKind(trace.SpanKindProducer))
}

// StartSpanFromHeaders will extract a span from the signature headers
// and start a new span with the given operation name.
func StartSpanFromHeaders(ctx context.Context, headers tasks.Headers, operationName string) (context.Context, trace.Span) {
	// Try to extract the span context from the carrier.
	propagator := otel.GetTextMapPropagator()
	ctx = propagator.Extract(ctx, propagation.HeaderCarrier(tasksToHTTPHeader(headers)))

	// Create a new span from the span context if found or start a new trace with the function name.
	// For clarity add the machinery component tag.
	spanContext, span := tracer.Start(ctx, operationName, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(MachineryTag)

	return spanContext, span
}

// HeadersWithSpan will inject a span into the signature headers
func HeadersWithSpan(ctx context.Context, headers tasks.Headers) tasks.Headers {
	propagator := otel.GetTextMapPropagator()
	httpHeaders := http.Header{}
	propagator.Inject(ctx, propagation.HeaderCarrier(httpHeaders))

	return applyTaskHeaders(headers, httpHeaders)
}

// AnnotateSpanWithSignatureInfo ...
func AnnotateSpanWithSignatureInfo(span trace.Span, signature *tasks.Signature) {
	// tag the span with some info about the signature
	span.SetAttributes(
		attribute.String("signature.name", signature.Name),
		attribute.String("signature.uuid", signature.UUID),
	)

	if signature.GroupUUID != "" {
		span.SetAttributes(attribute.String("signature.group.uuid", signature.GroupUUID))
	}

	if signature.ChordCallback != nil {
		span.SetAttributes(
			attribute.String("signature.chord.callback.uuid", signature.ChordCallback.UUID),
			attribute.String("signature.chord.callback.name", signature.ChordCallback.Name),
		)
	}
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(ctx context.Context, span trace.Span, chain *tasks.Chain) {
	// tag the span with some info about the chain
	span.SetAttributes(attribute.Int("chain.tasks.length", len(chain.Tasks)))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(ctx, signature.Headers)
	}
}

// AnnotateSpanWithGroupInfo ...
func AnnotateSpanWithGroupInfo(ctx context.Context, span trace.Span, group *tasks.Group, sendConcurrency int) {
	// tag the span with some info about the group
	span.SetAttributes(
		attribute.String("group.uuid", group.GroupUUID),
		attribute.Int("group.tasks.length", len(group.Tasks)),
		attribute.Int("group.concurrency", sendConcurrency),
	)

	// encode the task uuids to json, if that fails just dump it in
	span.SetAttributes(attribute.StringSlice("group.tasks", group.GetUUIDs()))

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(ctx, signature.Headers)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(ctx context.Context, span trace.Span, chord *tasks.Chord, sendConcurrency int) {
	// tag the span with chord specific info
	span.SetAttributes(attribute.String("chord.callback.uuid", chord.Callback.UUID))

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithSpan(ctx, chord.Callback.Headers)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(ctx, span, chord.Group, sendConcurrency)
}

func tasksToHTTPHeader(headers tasks.Headers) http.Header {
	result := http.Header{}

	for key, value := range headers {
		switch v := value.(type) {
		case string:
			result.Set(key, v)
		default:
			result.Set(key, fmt.Sprint(value))
		}
	}

	return result
}

func applyTaskHeaders(dest tasks.Headers, headers http.Header) tasks.Headers {
	if dest == nil {
		dest = tasks.Headers{}
	}

	for key, values := range headers {
		if len(values) == 0 {
			continue
		}

		dest.Set(key, values[0])
	}

	return dest
}
