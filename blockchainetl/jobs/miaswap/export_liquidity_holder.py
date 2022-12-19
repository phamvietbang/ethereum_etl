import logging

from web3 import Web3
from web3.middleware import geth_poa_middleware
from artifacts.abi.pancake_factory import PancakeFactoryABI
from artifacts.abi.lp_token import LP_TOKEN_ABI
from blockchainetl.streaming.exporter.s3_streaming_exporter import S3StreamingExporter
from blockchainetl.streaming.exporter.athena_s3_streaming_exporter import AthenaS3StreamingExporter

_LOGGER = logging.getLogger(__name__)


class ExportPairTokens:
    def __init__(self, chain, importer: AthenaS3StreamingExporter, exporter: S3StreamingExporter, provider, factory):
        self.importer = importer
        self.chain = chain
        self.factory = factory
        self.exporter = exporter
        self.w3 = Web3(Web3.HTTPProvider(provider))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.factory_contract = self.w3.eth.contract(address=self.factory, abi=PancakeFactoryABI)

    def get_holder_lp_tokens(self, holder):
        executed_id = self.importer.get_lp_token_holder(self.chain, holder)
        while not self.importer.has_query_succeeded(executed_id):
            break
        lp_token_data = self.importer.get_holder_data(executed_id)
        lp_tokens = []
        for token in lp_token_data:
            lp_tokens.append(token["lp_token"])

        return lp_tokens

    def get_liquidity_of_holder(self, holder):
        lp_tokens = self.get_holder_lp_tokens(holder)
        result = {}
        for token in lp_tokens:
            contract = self.w3.eth.contract(abi=LP_TOKEN_ABI, address=self.w3.toChecksumAddress(token))
            result[token] = contract.functions.balanceOf(self.w3.toChecksumAddress(holder)).call()

        return result


if __name__ == "__main__":
    # '0x66d899B8673f793507B6cd7B499688edc1711bc6'
    exporter = S3StreamingExporter(access_key="",
                                   secret_key="",
                                   bucket="bangbich123",
                                   aws_region="us-east-1")
    importer = AthenaS3StreamingExporter("",
                                         "",
                                         "bangbich123",
                                         "onus", "us-east-1")
    job = ExportPairTokens(importer=importer,
                           exporter=exporter,
                           chain="onus",
                           provider="https://rpc.onuschain.io/",
                           factory="0xA5DA4dC244c7aD33a0D8a10Ed5d8cFf078E86Ef3")

    job.get_liquidity_of_holder("0xfd409aAd331aCBfa938E7eb0ed598E25B3168E97")

