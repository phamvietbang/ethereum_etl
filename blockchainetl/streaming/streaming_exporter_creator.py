from blockchainetl.streaming.exporter.mongo_streaming_exporter import MongodbStreamingExporter


def create_steaming_exporter(output):
    streaming_exporter_type = determine_item_exporter_type(output)
    if streaming_exporter_type == StreamingExporterType.MONGODB:
        streaming_exporter = MongodbStreamingExporter(connection_url=output)
    else:
        streaming_exporter = None
    return streaming_exporter


def determine_item_exporter_type(output):
    if output is not None and output.startswith('mongodb'):
        return StreamingExporterType.MONGODB
    else:
        return StreamingExporterType.UNKNOWN


class StreamingExporterType:
    MONGODB = 'mongodb'
    UNKNOWN = 'unknown'
