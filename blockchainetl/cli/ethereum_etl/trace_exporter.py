# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import time

import click
from web3 import Web3
from web3.middleware import geth_poa_middleware
import logging
from blockchainetl.jobs.ethereum_etl.trace_exporter import ExportTracesJob
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter
from utils.logging_utils import logging_basic_config
from blockchainetl.providers.auto import get_provider_from_uri
from blockchainetl.utils.thread_local_proxy import ThreadLocalProxy

logging_basic_config()
_LOGGER = logging.getLogger(__name__)

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=0, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', required=True, type=int, help='End block')
@click.option('-b', '--batch-size', default=5, show_default=True, type=int,
              help='The number of blocks to filter at a time.')
@click.option('-o', '--output', default='-', show_default=True, type=str,
              help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('-p', '--provider-uri', required=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/.local/share/io.parity.ethereum/jsonrpc.ipc or http://localhost:8545/')
@click.option('--genesis-traces/--no-genesis-traces', default=False, show_default=True,
              help='Whether to include genesis traces')
@click.option('--daofork-traces/--no-daofork-traces', default=False, show_default=True,
              help='Whether to include daofork traces')
@click.option('-t', '--timeout', default=60, show_default=True, type=int, help='IPC or HTTP request timeout.')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def export_traces(start_block, end_block, batch_size, output, max_workers, provider_uri,
                  genesis_traces, daofork_traces, timeout=60, chain='ethereum'):
    """Exports traces from parity node."""
    if chain == 'classic' and daofork_traces:
        raise ValueError(
            'Classic chain does not include daofork traces. Disable daofork traces with --no-daofork-traces option.')
    w3 = Web3(ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, timeout=timeout)))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    start = time.time()
    job = ExportTracesJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        web3=w3,
        item_exporter=create_steaming_exporter(output=output),
        max_workers=max_workers,
        include_genesis_traces=genesis_traces,
        include_daofork_traces=daofork_traces)

    job.run()
    _LOGGER.info(f"crawl in {time.time() - start}")
