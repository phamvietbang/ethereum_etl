# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
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
import click
from blockchainetl.cli.create_pair_tx_exporter import export_create_pair_transactions
from blockchainetl.cli.event_exporter import export_events
from blockchainetl.cli.lp_token_exporter import export_lp_tokens
from blockchainetl.cli.holder_exporter import export_holders
from blockchainetl.cli.ethereum_etl.block_transaction_exporter import export_blocks_transactions
from blockchainetl.cli.ethereum_etl.log_exporter import export_logs
from blockchainetl.cli.ethereum_etl.trace_exporter import export_traces
from blockchainetl.cli.ethereum_etl.geth_trace_exporter import export_geth_traces
from blockchainetl.cli.ethereum_etl.stream_exporter import export_blocks_transactions_logs
from blockchainetl.cli.stream import stream


@click.group()
@click.version_option(version='1.6.3')
@click.pass_context
def cli(ctx):
    pass

cli.add_command(export_events, "export_events")
cli.add_command(export_lp_tokens, "export_lp_tokens")
cli.add_command(export_create_pair_transactions, "export_create_pair_transactions")
cli.add_command(export_holders, "export_holders")
cli.add_command(export_traces, "export_traces")
cli.add_command(export_logs, "export_logs")
cli.add_command(export_geth_traces, "export_geth_traces")
cli.add_command(export_blocks_transactions, "export_blocks_transactions")
cli.add_command(export_blocks_transactions_logs, "export_blocks_transactions_logs")
cli.add_command(stream, "stream")
