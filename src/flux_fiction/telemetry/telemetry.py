# tracing_setup.py
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

def get_tracer(service_name: str = "flux-fiction-python"):
    resource = Resource.create({
        "service.name": service_name,
    })

    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(
        OTLPSpanExporter(endpoint="http://127.0.0.1:4318/v1/traces")
    )
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    return trace.get_tracer(service_name)