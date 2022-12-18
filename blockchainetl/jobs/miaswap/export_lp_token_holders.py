import logging

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.streaming.exporter.athena_s3_streaming_exporter import AthenaS3StreamingExporter

_LOGGER = logging.getLogger(__name__)

NATIVE_ADDRESS = "0x0000000000000000000000000000000000000000"


class ExportMiaHolderJob(BaseJob):
    def __init__(
            self,
            chain,
            start_block,
            end_block,
            batch_size,
            max_workers,
            item_importer: AthenaS3StreamingExporter,
            item_exporter,
    ):
        self.chain = chain
        self.result = []
        self.item_exporter = item_exporter
        self.item_importer = item_importer
        self.end_block = end_block
        self.start_block = start_block
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

    def _start(self):
        self.item_exporter.open()
        self.item_importer.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self.export_batch
        )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.export_holders(self.chain, "addresses", self.result)
        self.item_exporter.close()
        self.item_importer.close()

    def export_batch(self, block_number_batch):
        _LOGGER.info(f'Get holder data from {block_number_batch[0]} to {block_number_batch[-1]}')
        executed_id = self.item_importer.get_log_by_block_number_and_hash(
            self.chain,
            block_number_batch[0],
            block_number_batch[-1],
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        )
        while not self.item_importer.has_query_succeeded(executed_id):
            break
        logs = self.item_importer.get_log_data(executed_id)
        tokens,  events, holders = [], [], []
        for event in logs:
            if event["address"] not in tokens:
                events.append(event)
                tokens.append(event["address"])

        executed_id = self.item_importer.get_lp_token_by_addresses(self.chain, set(tokens))
        while not self.item_importer.has_query_succeeded(executed_id):
            break
        lp_tokens = self.item_importer.get_lp_token_data(executed_id)

        lp_token_addresses = [lp_token["lp_token"] for lp_token in lp_tokens]
        for event in events:
            if event["address"] in lp_token_addresses:
                if event["topics"][1] != NATIVE_ADDRESS and event["topics"][1] not in lp_token_addresses:
                    self.result.append({
                        "lp_token": event["address"],
                        "address": event["topics"][1]
                    })
                if event["topics"][2] != NATIVE_ADDRESS and event["topics"][2] not in lp_token_addresses:
                    self.result.append({
                        "lp_token": event["address"],
                        "address": event["topics"][2]
                    })
