import logging

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob

_LOGGER = logging.getLogger(__name__)


class ExportLPTokenJob(BaseJob):
    def __init__(
            self,
            chain,
            item_importer,
            item_exporter,
            start_block,
            end_block,
            batch_size,
            max_workers,
            factory_address="0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
    ):
        self.chain = chain
        self.result = []
        self.factory_address = factory_address
        self.item_importer = item_importer
        self.item_exporter = item_exporter
        self.start_block = start_block
        self.end_block = end_block
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self.export_batch
        )

    def export_batch(self, block_number_batch):
        _LOGGER.info(f'Get lp token data from {block_number_batch[0]} to {block_number_batch[-1]}')
        _filter = {
            "block_number": {"$gte": block_number_batch[0], "$lte": block_number_batch[-1]},
            "to_address": self.factory_address
        }
        transactions = self.item_importer.get_items(self.chain, "transactions", _filter)
        for transaction in transactions:
            tmp = transaction.get("decoded_input")
            tmp["_id"] = tmp["pair"]
            tmp["created_block_number"] = transaction["block_number"]
            if tmp not in self.result:
                self.result.append(tmp)

    def _start(self):
        self.item_exporter.open()
        self.item_importer.open()

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.export_items(self.chain, "lp_tokens", self.result)
        self.item_exporter.close()
        self.item_importer.close()
