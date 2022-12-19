from eth_utils import keccak
from utils.utils import *
import logging
from blockchainetl.domain.receipt_log import EthReceiptLog
from blockchainetl.domain.lending_log_subcriber import EventSubscriber
from blockchainetl.domain.receipt_lending_log import EthEvent
from constants.structure_constants import Log

logger = logging.getLogger('EthLendingService')


class EthEventMapper(object):

    def build_list_info_event(self, abi):
        list_ = []
        for i in abi:
            arr = self.init_events_subscription(i)
            if not arr:
                continue
            else:
                list_.append(arr)
        return list_

    def init_events_subscription(self, abi):
        event_abi = abi
        if event_abi.get('type') == 'event':
            method_signature_hash = get_topic_filter(event_abi)
            list_params_in_order = get_list_params_in_order(event_abi)
            event_name = event_abi.get('name')
            event_subscriber = EventSubscriber(method_signature_hash, event_name, list_params_in_order)
            topic = method_signature_hash
            address_name_field = get_all_address_name_field(event_abi)
            return [event_subscriber, topic, address_name_field]
        return []

    def eth_event_to_dict(self, eth_event: EthEvent):
        d1 = {
            # 'type': 'event',
            # 'event_type': convert_even_type(eth_event.event_type),
            'contract_address': eth_event.contract_address,
            'transaction_hash': eth_event.transaction_hash,
            'log_index': eth_event.log_index,
            'block_number': eth_event.block_number,
        }
        d2 = eth_event.params
        return {**d1, **d2}

    def log_to_dict(self, log: EthReceiptLog):
        return {
            Log.address: log.address,
            Log.log_index: log.log_index,
            Log.data: log.data,
            Log.block_number: log.block_number,
            Log.transaction_index: log.transaction_index,
            Log.block_hash: log.block_hash,
            Log.topics: log.topics,
            Log.transaction_hash: log.transaction_hash
        }

    def web3_dict_to_receipt_log(self, dict):
        receipt_log = EthReceiptLog()
        receipt_log.log_index = dict.get('logIndex')
        receipt_log.transaction_index = dict.get('transactionIndex')
        transaction_hash = dict.get('transactionHash')
        if transaction_hash is not None:
            transaction_hash = transaction_hash.hex()
        receipt_log.transaction_hash = transaction_hash

        block_hash = dict.get('blockHash')
        if block_hash is not None:
            block_hash = block_hash.hex()
        receipt_log.block_hash = block_hash
        receipt_log.block_number = dict.get('blockNumber')
        receipt_log.address = dict.get('address')
        receipt_log.data = dict.get('data')

        if 'topics' in dict:
            receipt_log.topics = [topic.hex() for topic in dict['topics']]

        return receipt_log

    def decode_data_by_type(self, data, type):
        if self.is_integers(type):
            return hex_to_dec(data)
        elif type == "address":
            return word_to_address(data)
        else:
            return data

    def is_integers(self, type):
        return type in {"uint256", "uint128", "uint64", "uint32", "uint16", "uint8", "uint" \
            , "int256", "init128", "init64", "init32", "init16", "init8", "init"}

    def extract_event_from_log(self, receipt_log, event_subscriber):
        topics = receipt_log.topics
        if topics is None or len(topics) < 1:
            logger.warning("Topics are empty in log {} of transaction {}".format(receipt_log.log_index,
                                                                                 receipt_log.transaction_hash))
            return None
        if event_subscriber.topic_hash == topics[0]:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            list_params_in_order = event_subscriber.list_params_in_order
            # if the number of topics and fields in data part != len(list_params_in_order), then it's a weird event
            num_params = len(list_params_in_order)
            topics_with_data = topics_with_data[1:]
            if len(topics_with_data) != num_params:
                logger.warning("The number of topics and data parts is not equal to {} in log {} of transaction {}"
                               .format(str(num_params), receipt_log.log_index, receipt_log.transaction_hash))
                return None

            event = EthEvent()
            event.contract_address = to_normalized_address(receipt_log.address)
            event.transaction_hash = receipt_log.transaction_hash
            event.log_index = receipt_log.log_index
            event.block_number = receipt_log.block_number
            event.event_type = event_subscriber.name
            for i in range(num_params):
                param_i = list_params_in_order[i]
                name = param_i.get("name")
                type = param_i.get("type")
                data = topics_with_data[i]
                event.params[name] = self.decode_data_by_type(data, type)
            return event

        return None


# remove redundancy in topic
def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: '0x' + word, words))
        return words_with_0x
    return []


# convert topic to address
def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address('0x' + param[-40:])
    else:
        return to_normalized_address(param)


# hash abi to be topic
def get_topic_filter(event_abi):
    input_string = event_abi.get("name") + "("
    for input in event_abi.get("inputs"):
        input_string += input.get("type") + ","
    input_string = input_string[:-1] + ")"
    hash = keccak(text=input_string)
    return '0x' + hash.hex()


# get params from abi
def get_list_params_in_order(event_abi):
    indexed = []
    non_indexed = []
    for input in event_abi.get('inputs'):
        if input.get('indexed'):
            indexed.append(input)
        else:
            non_indexed.append(input)
    return indexed + non_indexed


def get_all_address_name_field(event_abi):
    address_name_field = []
    for input in event_abi.get('inputs'):
        if input.get('type') == 'address':
            address_name_field.append(input.get('name'))
    return address_name_field


def convert_even_type(event_type):
    event_type = event_type.upper()
    if event_type == 'LIQUIDATIONCALL':
        return 'LIQUIDATE'
    return event_type
