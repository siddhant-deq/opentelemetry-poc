from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.trace import SpanAttributes

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

import logging

import requests
import pandas as pd
import time
import random

def get_initialized_tracer():
    resource = Resource(attributes={"service.name": "Trial-App", "os-version": 1234.56, "cluster": "A", "datacentre": "BNE"})
    #Tracing Initialization
    COLLECTOR_ENDPOINT = "127.0.0.1"
    COLLECTOR_GRPC_PORT = 6004
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=f"http://{COLLECTOR_ENDPOINT}:{COLLECTOR_GRPC_PORT}", insecure=True))
    provider.add_span_processor(processor)
    # Sets the global default tracer provider
    trace.set_tracer_provider(provider)
    # Creates a tracer from the global tracer provider
    tracer = trace.get_tracer("my.tracer.name")
    return tracer