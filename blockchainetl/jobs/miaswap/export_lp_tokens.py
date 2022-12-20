import logging

from web3 import Web3
from web3.middleware import geth_poa_middleware
from artifacts.abi.pancake_factory import PancakeFactoryABI
from artifacts.abi.lp_token import LP_TOKEN_ABI
from blockchainetl.streaming.exporter.s3_streaming_exporter import S3StreamingExporter

_LOGGER = logging.getLogger(__name__)


class ExportPairTokens:
    def __init__(self, chain, exporter, provider, factory):
        self.chain = chain
        self.factory = factory
        self.exporter = exporter
        self.w3 = Web3(Web3.HTTPProvider(provider))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.factory_contract = self.w3.eth.contract(address=self.factory, abi=PancakeFactoryABI)

    def get_pair_information(self):
        all_pair_length = self.factory_contract.functions.allPairsLength().call()
        result = []
        for pid in range(all_pair_length):
            lp_token = self.factory_contract.functions.allPairs(pid).call()
            lp_contract = self.w3.eth.contract(address=lp_token, abi=LP_TOKEN_ABI)
            token_a = lp_contract.functions.token0().call()
            token_b = lp_contract.functions.token1().call()

            result.append({
                "lp_token": lp_token.lower(),
                "token_a": token_a.lower(),
                "token_b": token_b.lower(),
                # "pid": pid
            })
            print(f"Get data of lp token {lp_token}")

        self.exporter.export_lp_tokens(self.chain, "lp_tokens", result)


if __name__ == "__main__":
    exporter = S3StreamingExporter(access_key="AKIAXS2SFBSOTZFERP5O",
                                   secret_key="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", bucket="bangbich123",
                                   aws_region="us-east-1")
    job = ExportPairTokens(exporter=exporter, chain="onus", provider="https://rpc.onuschain.io/",
                           factory="0xA5DA4dC244c7aD33a0D8a10Ed5d8cFf078E86Ef3")
    job.get_pair_information()
