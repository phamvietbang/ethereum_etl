from constants.structure_constants import Transaction, Log, Trace
import boto3


class AthenaS3StreamingExporter(object):
    def __init__(self, access_key, secret_key, bucket, database, aws_region=None):
        self.bucket = bucket
        if aws_region:
            self.session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=aws_region
            )
        else:
            self.session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=aws_region
            )
        self.s3_client = self.session.client('s3')
        self.athena_client = self.session.client('athena')
        self.config = {'OutputLocation': f's3://{self.bucket}/config'}
        self.create_database(database)
        self.create_log_column(database)
        self.create_transactions_column(database)

    def create_database(self, database):
        self.athena_client.start_query_execution(
            QueryString=f'create database if not exists {database}',
            ResultConfiguration=self.config)

    def create_transactions_column(self, database):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.transactions (
            {Transaction.hash} STRING,
            {Transaction.nonce} BIGINT,
            {Transaction.block_hash} STRING,
            {Transaction.transaction_index} BIGINT,
            {Transaction.from_address} STRING,
            {Transaction.to_address} STRING,
            {Transaction.value} DECIMAL(38,0),
            {Transaction.gas} BIGINT,
            {Transaction.gas_price} BIGINT,
            {Transaction.input} STRING
        )
        PARTITIONED BY ({Transaction.block_number} BIGINT)
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION 's3://{self.bucket}/export/transactions/';
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_log_column(self, database):
        query = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS {database}.logs (
                id STRING,
                {Log.log_index} BIGINT,
                {Log.transaction_hash} STRING,
                {Log.transaction_index} BIGINT,
                {Log.block_hash} STRING,
                {Log.address} STRING,
                {Log.data} STRING,
                {Log.topics} ARRAY<STRING>
            )
            PARTITIONED BY ({Log.block_number} BIGINT)
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            LOCATION 's3://{self.bucket}/export/logs/';
            '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_trace_column(self, database):
        pass

    def export_items(self, database, type_, data):
        if type_ == "transactions":
            self.export_transactions(database, data)

        if type_ == "logs":
            self.export_logs(database, data)

    def export_transactions(self, database, data):
        query = f'''
        INSERT INTO {database}.transactions
        VALUES
        '''
        for transaction in data:
            query += f'''(
            '{transaction[Transaction.hash]}',
            {transaction[Transaction.nonce]},
            '{transaction[Transaction.block_hash]}',
            {transaction[Transaction.transaction_index]},
            '{transaction[Transaction.from_address]}',
            '{transaction[Transaction.to_address]}',
            {transaction[Transaction.value]},
            {transaction[Transaction.gas]},
            {transaction[Transaction.gas_price]},
            '{transaction[Transaction.input]}',
            {transaction[Transaction.block_number]}
            ),'''

        query = query[:-1]
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def export_logs(self, database, data):
        query = f'''
        INSERT INTO {database}.logs
        VALUES
        '''
        for log in data:
            query += f'''(
                '{log[Log.block_number]}_{log[Log.log_index]}',
                {log[Log.log_index]},
                '{log[Log.transaction_hash]}',
                {log[Log.transaction_index]},
                '{log[Log.block_hash]}',
                '{log[Log.address]}',
                '{log[Log.data]}',
                array {log[Log.topics]},
                {log[Log.block_number]}
            ),'''

        query = query[:-1]
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def close(self):
        pass

    def open(self):
        pass
