import json

from data_storage.memory_storage import MemoryStorage
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from services.json_rpc_requests import generate_get_block_by_number_json_rpc
from blockchainetl.mappers.block_mapper import EthBlockMapper
from blockchainetl.mappers.transaction_mapper import EthTransactionMapper
from blockchainetl.utils.utils import rpc_response_batch_to_results, validate_range


# Exports blocks and transactions
class ExportBlocksJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=True):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError('At least one of export_blocks or export_transactions must be True')

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        self.contract_filter = MemoryStorage.getInstance()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch
        )

    def _export_batch(self, block_number_batch):
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]
        for block in blocks:
            self._export_block(block)

    def _export_block(self, block):
        if self.export_blocks:
            self.item_exporter.export_item(self.block_mapper.block_to_dict(block))

        if self.export_transactions:
            for tx in block.transactions:
                tx_dict = self.transaction_mapper.transaction_to_dict(tx)
                self.item_exporter.export_item(tx_dict)
                if tx_dict.get("input") != "0x" and not self.contract_filter.exited(tx_dict.get("to_address")):
                    if not tx_dict.get("to_address"):
                        continue
                    self.contract_filter.add_temp(tx_dict.get("to_address"))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
