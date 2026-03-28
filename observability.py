"""
OpenTelemetry instrumentation for tracing, metrics, and log correlation.

Exports traces and logs to Grafana Cloud (or any OTLP-compatible backend).
Auto-instruments FastAPI, httpx, Python logging, and Neo4j driver queries.
No changes to database code required.

Configuration (environment variables):
    OTEL_ENABLED            - set to "true" to activate (default: false)
    OTEL_SERVICE_NAME       - logical service name shown in Grafana (default: openpecha-api)
    OTEL_EXPORTER_OTLP_ENDPOINT - Grafana Cloud OTLP endpoint, e.g.
                                  https://otlp-gateway-prod-us-east-0.grafana.net/otlp
    OTEL_EXPORTER_OTLP_HEADERS  - auth header, e.g.
                                  Authorization=Basic <base64(instanceId:token)>
"""

import functools
import logging
import os
import re
from typing import TYPE_CHECKING, Any, LiteralString, cast

from neo4j import AsyncResult, AsyncSession, Query
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import StatusCode

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


_state: dict[str, bool] = {"neo4j_patched": False}


def _is_enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "false").lower() == "true"


def _extract_operation(query: str) -> str:
    normalized = " ".join(query.strip().split())
    for keyword in ("MATCH", "CREATE", "MERGE", "DELETE", "RETURN", "CALL", "WITH"):
        if normalized.upper().startswith(keyword):
            # Try to extract the node label, e.g. MATCH (e:Text ...) -> "MATCH Text"
            label_match = re.search(r"\(\w*:(\w+)", normalized)
            if label_match:
                return f"{keyword} {label_match.group(1)}"
            return keyword
    return "query"


def _instrument_neo4j_driver() -> None:
    if _state["neo4j_patched"]:
        return

    original_run = AsyncSession.run

    @functools.wraps(original_run)
    async def traced_run(
        self: AsyncSession, query: LiteralString | Query, parameters: dict[str, Any] | None = None, **kwargs: object
    ) -> AsyncResult:
        tracer = trace.get_tracer("openpecha-api")
        query_str = str(query)
        operation = _extract_operation(query_str)

        # Truncate query for the span attribute (avoid huge attributes)
        truncated_query = query_str[:500] + "..." if len(query_str) > 500 else query_str

        with tracer.start_as_current_span(
            f"neo4j {operation}",
            kind=trace.SpanKind.CLIENT,
            attributes={
                "db.system": "neo4j",
                "db.name": "neo4j",
                "db.operation.name": operation,
                "db.query.text": truncated_query,
            },
        ) as span:
            try:
                return await original_run(self, query, parameters, **kwargs)
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                raise

    AsyncSession.run = cast("Any", traced_run)
    _state["neo4j_patched"] = True
    logger.info("Neo4j AsyncSession.run() instrumented for tracing")


def setup_telemetry(app: FastAPI) -> None:
    if not _is_enabled():
        logger.info("OpenTelemetry disabled (set OTEL_ENABLED=true to activate)")
        return

    service_name = os.getenv("OTEL_SERVICE_NAME", "openpecha-api")
    environment = os.getenv("ENVIRONMENT", "development")

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": "2.2.0",
            "deployment.environment": environment,
        }
    )

    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint:
        otlp_exporter = OTLPSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        logger.info("OpenTelemetry exporting traces to %s", otlp_endpoint)
    else:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        logger.info("OpenTelemetry exporting traces to console (no OTLP endpoint configured)")

    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls="__/health,docs,redoc,openapi.json",
    )

    HTTPXClientInstrumentor().instrument()

    LoggingInstrumentor().instrument(
        log_hook=_log_hook,
        set_logging_format=True,
    )

    _instrument_neo4j_driver()

    logger.info(
        "OpenTelemetry initialized: service=%s, environment=%s",
        service_name,
        environment,
    )


def shutdown_telemetry() -> None:
    """Flush and shut down the tracer provider. Call during app shutdown."""
    if not _is_enabled():
        return

    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        provider.shutdown()
        logger.info("OpenTelemetry shut down")


def _log_hook(span: trace.Span, record: logging.LogRecord) -> None:
    if span and span.is_recording():
        ctx = span.get_span_context()
        record.otelTraceID = format(ctx.trace_id, "032x")
        record.otelSpanID = format(ctx.span_id, "016x")
