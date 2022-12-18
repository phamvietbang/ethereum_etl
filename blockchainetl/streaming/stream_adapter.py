import os
import pathlib

from web3 import Web3
from web3.middleware import geth_poa_middleware
from blockchainetl.jobs.event_exporter import ExportEventJob
from blockchainetl.jobs.holder_exporter import ExportHolderJob
from blockchainetl.jobs.lp_token_exporter import ExportLPTokenJob
from blockchainetl.jobs.create_pair_tx_exporter import ExportCreatePairTxJob
from constants.job_constant import Job
from artifacts.abi.events.transfer_event_abi import TRANSFER_EVENT_ABI


class StreamAdapter:
    def __init__(
            self,
            chain,
            item_importer,
            item_exporter,
            provider,
            batch_size=96,
            max_workers=8,
            event_abi=None,
            factory_abi=None,
            collector_id=None,
            contract_addresses=None,
            type_=Job.TRANSACTION
    ):
        self.type = type_
        self.chain = chain
        self.item_importer = item_importer
        self.event_abi = event_abi
        self.factory_abi = factory_abi
        self.provider = provider
        self.w3 = Web3(provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.contract_addresses = contract_addresses
        self.collector_id = collector_id

    def open(self):
        self.item_importer.open()
        self.item_exporter.open()

    def get_current_block_number(self):
        block_number = None
        if self.collector_id:
            collector = self.item_importer.get_collector(self.chain, collector_id=self.collector_id)
            block_number = collector.get('last_updated_at_block_number')
        if not block_number:
            block_number = self.w3.eth.blockNumber
        return int(block_number)

    def export_all(self, start_block, end_block):
        # Extract token transfers
        self._export_token_transfers(start_block, end_block)

    def _export_token_transfers(self, start_block, end_block):
        if Job.EVENT in self.type:
            if not self.event_abi:
                self.event_abi = TRANSFER_EVENT_ABI
            job = ExportEventJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                provider=self.provider,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
                contract_addresses=self.contract_addresses,
                abi=self.event_abi,
            )
            job.run()
        if Job.TRANSACTION in self.type:
            job = ExportCreatePairTxJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                batch_web3_provider=self.provider,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
            )
            job.run()
        if Job.LP_TOKEN in self.type:
            job = ExportLPTokenJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                item_importer=self.item_importer,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
            )
            job.run()
        if Job.ADDRESS in self.type:
            job = ExportHolderJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                item_importer=self.item_importer,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
            )
            job.run()

    def close(self):
        self.item_exporter.close()
