import logging

from blockchainetl.jobs.block_transaction_exporter import ExportBlocksJob
from artifacts.abi.pancake_factory import PancakeFactoryABI
from web3 import Web3
from web3.middleware import geth_poa_middleware

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
            export_transactions=True
    ):
        super().__init__(start_block, end_block, batch_size, batch_web3_provider, max_workers, item_exporter,
                         export_blocks, export_transactions)
        self.chain = chain
        self.result = []
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.pancake_factory_contract = self.w3.eth.contract(
            address=self.w3.toChecksumAddress("0xca143ce32fe78f1f7019d7d551a6402fc5350c73"),
            abi=PancakeFactoryABI)

    def _export_block(self, block):
        _LOGGER.info(f'Get tx data from {block[0]} to {block[-1]}')
        for tx in block.transactions:
            tx_dict = self.transaction_mapper.transaction_to_dict(tx)
            tx_dict["_id"] = tx_dict["hash"]
            if tx_dict.get("to_address") == "0xca143ce32fe78f1f7019d7d551a6402fc5350c73":
                func_obj, func_params = self.pancake_factory_contract.decode_function_input(tx_dict.get("input"))
                pair = self.pancake_factory_contract.functions.getPair(
                    func_params["tokenA"], func_params["tokenB"]
                ).call()
                if func_obj.fn_name == "createPair":
                    tx_dict["decoded_input"] = {
                        "token_a": func_params["tokenA"].lower(),
                        "token_b": func_params["tokenB"].lower(),
                        "pair": pair.lower()
                    }
            self.result.append(tx_dict)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.export_items(self.chain, "transactions", self.result)
        self.item_exporter.close()
