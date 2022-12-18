import logging
import time
import click
from utils.logging_utils import logging_basic_config
from blockchainetl.providers.auto import get_provider_from_uri
from blockchainetl.utils.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter
from blockchainetl.streaming.eth_stream_adapter import EthStreamAdapter
from blockchainetl.streaming.streamer import Streamer


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-block-file', default='last_synced_block.txt', show_default=True, type=str, help='')
@click.option('--lag', default=0, show_default=True, type=int, help='The number of blocks to lag behind the network.')
@click.option('--log-file', default=None, show_default=True, type=str, help='Log file')
@click.option('--pid-file', default=None, show_default=True, type=str, help='pid file')
@click.option('--period-seconds', default=1, show_default=True, type=int,
              help='How many seconds to sleep between syncs')
@click.option('-s', '--start-block', default=None, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int,
              help='The number of blocks to export at a time.')
@click.option('-B', '--streamer_batch_size', default=400, show_default=True, type=int,
              help='The number of blocks to collect at a time.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-w', '--max-workers', default=4, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--input-db', default=None, show_default=True, type=str,
              help='input database')
@click.option('--output', default=None, show_default=True, type=str,
              help='output database')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='The chain network to connect to.')
@click.option('--stream-type', default=None, show_default=True, type=str, help='Run task.')
@click.option('--export-block', default=False, show_default=True, type=bool, help='export block data')
@click.option('--export-transaction', default=True, show_default=True, type=bool, help='export tx data')
@click.option('--export-receipt', default=False, show_default=True, type=bool, help='export receipt data')
@click.option('--export-log', default=True, show_default=True, type=bool, help='export log data')
def stream_eth(last_synced_block_file, lag, log_file, pid_file, period_seconds, start_block, end_block,
               batch_size, streamer_batch_size, provider_uri, max_workers, input_db, output, chain,
               stream_type, export_block, export_transaction, export_receipt, export_log):
    """Collect token transfer events."""
    logging_basic_config(filename=log_file)
    logger = logging.getLogger('Streamer')

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    provider_uri = provider_uri
    logger.info(f"Start streaming from block {start_block} to block {end_block}")

    logger.info('Using node: ' + provider_uri)
    streamer_adapter = EthStreamAdapter(
        chain=chain,
        provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_importer=create_steaming_exporter(output=input_db),
        item_exporter=create_steaming_exporter(output=output),
        batch_size=batch_size,
        max_workers=max_workers,
        types=stream_type,
        export_blocks=export_block,
        export_log=export_log,
        export_receipt=export_receipt,
        export_transactions=export_transaction,
    )
    streamer = Streamer(
        chain=chain,
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=streamer_batch_size,
        pid_file=pid_file,
        output=output
    )
    start_time = int(time.time())
    streamer.stream()
    end_time = int(time.time())
    logging.info('Total time ' + str(end_time - start_time))
