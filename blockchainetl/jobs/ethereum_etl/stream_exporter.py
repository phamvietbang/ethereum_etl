import json
import logging
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.jobs.ethereum_etl.block_transaction_exporter import ExportBlocksJob
from blockchainetl.jobs.ethereum_etl.receipt_log_exporter import ExportReceiptsJob
from services.json_rpc_requests import generate_get_block_by_number_json_rpc
from utils.utils import rpc_response_batch_to_results

_LOGGER = logging.getLogger(__name__)


class ExportBlockTxReceiptJob(ExportBlocksJob):
    def __init__(self,
                 chain,
                 start_block,
                 end_block,
                 batch_size,
                 batch_web3_provider,
                 max_workers,
                 item_exporter,
                 export_receipts=True,
                 export_logs=True,
                 export_blocks=True,
                 export_transactions=True
                 ):
        super().__init__(
            chain=chain,
            start_block=start_block,
            end_block=end_block,
            batch_size=batch_size,
            batch_web3_provider=batch_web3_provider,
            max_workers=max_workers,
            item_exporter=item_exporter,
            export_blocks=export_blocks,
            export_transactions=export_transactions
        )
        self.batch_size = batch_size
        self.max_worker = max_workers
        self.export_receipts = export_receipts
        self.export_logs = export_logs

    def _export_batch(self, block_number_batch):
        _LOGGER.info(f'Start crawl data from {block_number_batch[0]} to {block_number_batch[-1]}')
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]
        txs = []
        for block in blocks:
            txs += self._export_block(block)
        if txs:
            self.item_exporter.export_items(self.chain, "transactions", txs,
                                            f'{block_number_batch[0]}_{block_number_batch[-1]}')
            if self.export_receipts or self.export_logs:
                transaction_hash = [i["hash"] for i in txs]
                job = ExportReceiptsJob(
                    chain=self.chain,
                    transaction_hashes_iterable=transaction_hash,
                    batch_size=self.batch_size,
                    batch_web3_provider=self.batch_web3_provider,
                    max_workers=self.max_worker,
                    item_exporter=self.item_exporter,
                    export_receipts=self.export_receipts,
                    export_logs=self.export_logs,
                )
                job.run()
