import logging

from blockchainetl.jobs.ethereum_etl.block_transaction_exporter import ExportBlocksJob
from artifacts.abi.pancake_factory import PancakeFactoryABI

_LOGGER = logging.getLogger(__name__)


class ExportCreatePairTxJob(ExportBlocksJob):
    def __init__(
            self,
            chain,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=False,
            export_transactions=True,
            factory="0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
    ):
        super().__init__(chain, start_block, end_block, batch_size, batch_web3_provider, max_workers, item_exporter,
                         export_blocks, export_transactions)
        self.chain = chain
        self.result = []
        self.factory = factory.lower()
        self.lp_token = []
        self.pancake_factory_contract = self.w3.eth.contract(
            address=self.w3.toChecksumAddress(factory),
            abi=PancakeFactoryABI)

    def _export_block(self, block):
        for tx in block.transactions:
            tx_dict = self.transaction_mapper.transaction_to_dict(tx)
            tx_dict["_id"] = tx_dict["hash"]
            if tx_dict.get("to_address") == self.factory:
                func_obj, func_params = self.pancake_factory_contract.decode_function_input(tx_dict.get("input"))
                if func_obj.fn_name == "createPair":
                    pair = self.pancake_factory_contract.functions.getPair(
                        func_params["tokenA"], func_params["tokenB"]
                    ).call()
                    self.lp_token.append({
                        "token_a": func_params["tokenA"].lower(),
                        "token_b": func_params["tokenB"].lower(),
                        "lp_token": pair.lower()
                    })
                self.result.append(tx_dict)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.export_items(self.chain, "transactions", self.result)
        self.item_exporter.export_items(self.chain, "lp_tokens", self.lp_token)
        self.item_exporter.close()
