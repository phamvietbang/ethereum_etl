import logging
from web3 import Web3
from web3.middleware import geth_poa_middleware
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.streaming.exporter.athena_s3_streaming_exporter import AthenaS3StreamingExporter
from artifacts.abi.erc20_abi import ERC20_ABI
from blockchainetl.streaming.exporter.s3_streaming_exporter import S3StreamingExporter

_LOGGER = logging.getLogger(__name__)


class ExportERC20TokenJob(BaseJob):
    def __init__(
            self,
            chain,
            provider,
            start_block,
            end_block,
            batch_size,
            max_workers,
            item_importer: AthenaS3StreamingExporter,
            item_exporter,
    ):
        self.w3 = Web3(Web3.HTTPProvider(provider))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
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
        self.item_exporter.close()
        self.item_importer.close()

    def export_batch(self, block_number_batch):
        _LOGGER.info(f'Get token data from {block_number_batch[0]} to {block_number_batch[-1]}')
        # print(f'Get token data from {block_number_batch[0]} to {block_number_batch[-1]}\n')
        executed_id = self.item_importer.get_log_by_block_number_and_hash(
            self.chain,
            block_number_batch[0],
            block_number_batch[-1],
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        )
        while not self.item_importer.has_query_succeeded(executed_id):
            break
        logs = self.item_importer.get_log_data(executed_id)
        events, result = [], []
        for event in logs:
            if event["address"] not in self.result:
                events.append(event)
                contract = self.w3.eth.contract(address=self.w3.toChecksumAddress(event["address"]), abi=ERC20_ABI)
                try:
                    total_supply = contract.functions.totalSupply().call()
                    decimals = contract.functions.decimals().call()
                    result.append({
                        "address": event["address"],
                        "supply": str(total_supply),
                        "decimals": decimals
                    })
                except:
                    pass

        self.item_exporter.export_tokens(self.chain, "tokens", result)

if __name__ == "__main__":
    exporter = S3StreamingExporter(access_key="",
                                   secret_key="",
                                   bucket="bangbich123",
                                   aws_region="us-east-1")
    importer = AthenaS3StreamingExporter("",
                                         "",
                                         "bangbich123",
                                         "onus", "us-east-1")
    job = ExportERC20TokenJob(
        chain="onus",
        provider="https://rpc.onuschain.io/",
        start_block=407514,
        end_block=407606,
        batch_size=10,
        max_workers=4,
        item_importer=importer,
        item_exporter=exporter
    )
    job.run()
