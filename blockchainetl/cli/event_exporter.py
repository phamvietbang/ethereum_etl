import click

from blockchainetl.jobs.events.event_exporter import ExportEventJob
from utils.logging_utils import logging_basic_config
from blockchainetl.providers.auto import get_provider_from_uri
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter
from blockchainetl.utils.thread_local_proxy import ThreadLocalProxy
from artifacts.abi.events.transfer_event_abi import TRANSFER_EVENT_ABI
import json

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=0, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int,
              help='The number of blocks to export at a time.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--output', default=None, show_default=True, type=str,
              help='output database')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='The chain network to connect to.')
@click.option('-a', '--abi', default=None, show_default=True, type=str, help='The abi link.')
def export_events(start_block, end_block, batch_size, provider_uri, max_workers,
                  output, chain, abi):
    """Exports blocks and transactions."""
    if abi:
        with open(abi, "r") as f:
            _abi = json.loads(f.read())
    else:
        _abi = TRANSFER_EVENT_ABI

    if output is None:
        raise ValueError('Either --blocks-output or --transactions-output options must be provided')
    item = create_steaming_exporter(output=output)
    job = ExportEventJob(
        chain=chain,
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=item,
        abi=_abi)
    job.run()
