from blockchainetl.streaming.exporter.mongo_streaming_exporter import MongodbStreamingExporter
from blockchainetl.streaming.exporter.s3_streaming_exporter import S3StreamingExporter
from blockchainetl.streaming.exporter.athena_s3_streaming_exporter import AthenaS3StreamingExporter


def create_steaming_exporter(output):
    streaming_exporter_type = determine_item_exporter_type(output)
    if streaming_exporter_type == StreamingExporterType.MONGODB:
        streaming_exporter = MongodbStreamingExporter(connection_url=output)
    elif streaming_exporter_type == StreamingExporterType.S3:
        items = output.split("@")
        region = None
        if len(items) >= 5:
            region = items[-1]
        streaming_exporter = S3StreamingExporter(items[1], items[2], items[3], region)
    elif streaming_exporter_type == StreamingExporterType.ATHENA:
        items = output.split("@")
        region = None
        if len(items) >= 6:
            region = items[-1]
        streaming_exporter = AthenaS3StreamingExporter(items[1], items[2], items[3], items[4], region)
    else:
        streaming_exporter = None
    return streaming_exporter


def determine_item_exporter_type(output):
    if output is not None and output.startswith('mongodb'):
        return StreamingExporterType.MONGODB
    elif output is not None and output.startswith('s3'):
        return StreamingExporterType.S3
    elif output is not None and output.startswith('athena'):
        return StreamingExporterType.ATHENA
    else:
        return StreamingExporterType.UNKNOWN


class StreamingExporterType:
    MONGODB = 'mongodb'
    S3 = 's3'
    ATHENA = 'ATHENA'
    UNKNOWN = 'unknown'
