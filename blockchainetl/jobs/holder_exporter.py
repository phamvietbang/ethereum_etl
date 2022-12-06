import logging

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob

_LOGGER = logging.getLogger(__name__)

NATIVE_ADDRESS = "0x0000000000000000000000000000000000000000"


class ExportHolderJob(BaseJob):
    def __init__(
            self,
            chain,
            start_block,
            end_block,
            batch_size,
            max_workers,
            item_importer,
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
        self.item_exporter.export_items(self.chain, "addresses", self.result)
        self.item_exporter.close()
        self.item_importer.close()

    def export_batch(self, block_number_batch):
        _LOGGER.info(f'Get holder data from {block_number_batch[0]} to {block_number_batch[-1]}')
        _filter = {
            "event_type": "TRANSFER",
            "block_number": {
                "$gte": block_number_batch[0],
                "$lte": block_number_batch[-1]
            }
        }
        logs = self.item_importer.get_items(self.chain, "logs", _filter)
        tokens,  events = [], []
        for event in logs:
            if event["contract_address"] not in tokens:
                events.append(event)
                tokens.append(event["contract_address"])

        _filter = {
            "_id": {"$in": tokens}
        }
        lp_tokens = self.item_importer.get_items(self.chain, "lp_tokens", _filter)
        lp_token_addresses = [lp_token["_id"] for lp_token in lp_tokens]
        address_ids = []
        for event in events:
            if event["contract_address"] in lp_token_addresses:
                if event["from"] != NATIVE_ADDRESS and event["from"] not in lp_token_addresses:
                    address_ids.append(f'{event["from"]}_{event["contract_address"]}')
                    self.result.append({
                        "_id": f'{event["from"]}_{event["contract_address"]}',
                        "pair": event["contract_address"],
                        "address": event["from"]
                    })
                if event["to"] != NATIVE_ADDRESS and event["from"] not in lp_token_addresses:
                    address_ids.append(f'{event["from"]}_{event["contract_address"]}')
                    self.result.append({
                        "_id": f'{event["to"]}_{event["contract_address"]}',
                        "pair": event["contract_address"],
                        "address": event["to"]
                    })
