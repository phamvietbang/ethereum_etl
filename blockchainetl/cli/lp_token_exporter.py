import click

from blockchainetl.jobs.lp_token_exporter import ExportLPTokenJob
from utils.logging_utils import logging_basic_config
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=0, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int,
              help='The number of blocks to export at a time.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--input-db', default=None, show_default=True, type=str,
              help='input database')
@click.option('--output', default=None, show_default=True, type=str,
              help='output database')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='The chain network to connect to.')
@click.option('-f', '--factory-address', default=None, show_default=True, type=str, help='factory address')
def export_lp_tokens(start_block, end_block, batch_size, max_workers,
                     input_db, output, chain, factory_address):
    """Exports blocks and transactions."""
    if output is None:
        raise ValueError('Either output options must be provided')
    if input_db is None:
        raise ValueError('Either input options must be provided')
    if not factory_address:
        factory_address = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
    item_importer = create_steaming_exporter(output=input_db)
    item_exporter = create_steaming_exporter(output=output)
    job = ExportLPTokenJob(
        chain=chain,
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        max_workers=max_workers,
        item_importer=item_importer,
        item_exporter=item_exporter,
        factory_address=factory_address
    )
    job.run()
