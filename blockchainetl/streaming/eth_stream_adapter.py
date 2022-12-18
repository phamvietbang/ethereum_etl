import os
import pathlib

from web3 import Web3
from web3.middleware import geth_poa_middleware
from blockchainetl.jobs.ethereum_etl.block_transaction_exporter import ExportBlocksJob
from blockchainetl.jobs.ethereum_etl.receipt_log_exporter import ExportReceiptsJob
from blockchainetl.jobs.ethereum_etl.log_exporter import ExportLogJob


class EthStreamAdapter:
    def __init__(
            self,
            chain,
            item_importer,
            item_exporter,
            provider,
            types,
            batch_size=96,
            max_workers=8,
            export_blocks=False,
            export_transactions=True,
            export_receipt=False,
            export_log=True
    ):
        self.export_log = export_log
        self.export_receipt = export_receipt
        self.types = types
        self.export_transactions = export_transactions
        self.export_blocks = export_blocks
        self.chain = chain
        self.item_importer = item_importer
        self.provider = provider
        self.w3 = Web3(provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers

    def open(self):
        self.item_importer.open()
        self.item_exporter.open()

    def get_current_block_number(self):
        block_number = self.w3.eth.blockNumber
        return int(block_number)

    def export_all(self, start_block, end_block):
        # Extract token transfers
        self._export_token_transfers(start_block, end_block)

    def _export_token_transfers(self, start_block, end_block):
        transactions = []
        if "block_transaction" in self.types:
            job = ExportBlocksJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                batch_web3_provider=self.provider,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
                export_blocks=self.export_blocks,
                export_transactions=self.export_transactions,
            )
            job.run()
        if "receipt" in self.types and transactions:
            transaction_hashes = [i["hash"] for i in transactions]
            job = ExportReceiptsJob(
                chain=self.chain,
                transaction_hashes_iterable=transaction_hashes,
                batch_size=self.batch_size,
                batch_web3_provider=self.provider,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
                export_receipts=self.export_receipt,
                export_logs=self.export_log
            )
            job.run()
        if "logs" in self.types:
            job = ExportLogJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                provider=self.provider,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
                contract_addresses=None
            )
            job.run()

    def close(self):
        self.item_exporter.close()
