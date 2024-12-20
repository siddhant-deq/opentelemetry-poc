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

#Resource
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

#Metrics Initialization
metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"http://{COLLECTOR_ENDPOINT}:{COLLECTOR_GRPC_PORT}", insecure=True))
console_reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
provider = MeterProvider(metric_readers=[metric_reader, console_reader])
# Sets the global default meter provider
metrics.set_meter_provider(provider)
# Creates a meter from the global meter provider
meter = metrics.get_meter("my.meter.name")


#Measuring Instruments
api_row_gauge = meter.create_gauge(
    "api.row.count.gauge", unit="1", description="Counts the number of rows returned by a data api request"
)
write_row_gauge = meter.create_gauge(
    "write.row.count.gauge", unit="1", description="Counts the number of rows written to a csv file"
)


def set_attributes(*attrs):
    span = trace.get_current_span()
    combined = {}
    for attr in attrs:
        combined.update(attr)
    span.set_attributes(combined)

def emit_event(name, attributes, timestamp=time.time_ns()):
    span = trace.get_current_span()
    span.add_event(name=name, attributes=attributes, timestamp=timestamp)

@tracer.start_as_current_span("fetch_data")
def fetch_data(url):
    set_attributes({SpanAttributes.HTTP_URL: url})
    emit_event("Fetching data from API", attributes={SpanAttributes.HTTP_URL: url})
    response = requests.get(url)
    emit_event("Data received", attributes={SpanAttributes.HTTP_URL: url})
    json_data = response.json()
    data = json_data['data']
    api_row_gauge.set(len(data), attributes={SpanAttributes.FAAS_TIME: time.time_ns()})
    return data

@tracer.start_as_current_span("process_data_to_df")
def process_data_to_df(data):
    df_orig = pd.DataFrame(data)
    df_orig = df_orig.drop(columns=['time_until_update'])
    df_orig.columns = ['alternativeme_fear_greed_index_1d', 'alternativeme_fear_greed_class_1d', 'event_timestamp']
    df_orig['event_timestamp'] = pd.to_datetime(df_orig['event_timestamp'].astype(int), unit='s')
    return df_orig

@tracer.start_as_current_span("process_df")
def process_df(df, sleep_time=0):
    df['final_str'] = ''
    for index, row in df.iterrows():
        df.iloc[index] = process_row(row, sleep_time=sleep_time)
    return df

@tracer.start_as_current_span("process_row")
def process_row(row, sleep_time=0):
    time.sleep(sleep_time)
    row['final_str'] = row['alternativeme_fear_greed_class_1d'] + row['alternativeme_fear_greed_index_1d']
    return row

@tracer.start_as_current_span("write_to_csv")
def write_to_csv(df, filename):
    set_attributes({SpanAttributes.CODE_FILEPATH: filename})
    emit_event("Writing to csv", attributes={SpanAttributes.CODE_FILEPATH: filename})
    df.to_csv(filename, index=False)
    write_row_gauge.set(len(df), attributes={SpanAttributes.FAAS_TIME: time.time_ns()})
    emit_event("Written to csv", attributes={SpanAttributes.CODE_FILEPATH: filename})

if __name__ == "__main__":
    with tracer.start_as_current_span("fear-greed-index-pipeline"):
        data = fetch_data(url='https://api.alternative.me/fng/?limit=0&format=json')
        df = process_data_to_df(data)
        df = process_df(df)
        write_to_csv(df, filename='fear_n_greed_index.csv')
        index = -random.randint(1, 6)
        if index == 6:
            trace.get_current_span().set_status(trace.Status(1, "Error: highest value encountered"))
            trace.get_current_span().record_exception(index)
        trace.get_current_span().set_attribute("result", "{}".format(df.iloc[index]['final_str']))
        trace.get_current_span().set_status(trace.StatusCode.OK)