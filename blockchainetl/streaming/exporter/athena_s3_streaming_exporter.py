import json
import time

from constants.structure_constants import Transaction, Log, Trace
import boto3


class AthenaS3StreamingExporter(object):
    def __init__(self, access_key, secret_key, bucket, database, aws_region=None):
        self.database = database
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
        self.create_database()
        self.create_log_table()
        self.create_transaction_table()
        self.create_erc20_token_table()
        self.create_lp_token_holder_table()
        self.create_lp_token_table()

    def create_database(self):
        self.athena_client.start_query_execution(
            QueryString=f'create database if not exists {self.database}',
            ResultConfiguration=self.config)

    def create_erc20_token_table(self):
        query = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`tokens` (
              `address` string,
              `supply` string,
              `decimals` int
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              'ignore.malformed.json' = 'FALSE',
              'dots.in.keys' = 'FALSE',
              'case.insensitive' = 'TRUE',
              'mapping' = 'TRUE'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{self.bucket}/{self.database}/tokens/'
            TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_lp_token_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`lp_tokens` (
          `lp_token` string,
          `token_a` string,
          `token_b` string,
          `pid` bigint
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'FALSE',
          'dots.in.keys' = 'FALSE',
          'case.insensitive' = 'TRUE',
          'mapping' = 'TRUE'
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{self.bucket}/{self.database}/lp_tokens/'
        TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_lp_token_holder_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`holders` (
          `lp_token` string,
          `address` string
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'FALSE',
          'dots.in.keys' = 'FALSE',
          'case.insensitive' = 'TRUE',
          'mapping' = 'TRUE'
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{self.bucket}/{self.database}/holders/'
        TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_transaction_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`transactions` (
          `hash` string,
          `nonce` bigint,
          `block_hash` string,
          `block_number` bigint,
          `block_timestamp` bigint,
          `transaction_index` bigint,
          `from_address` string,
          `to_address` string,
          `value` string,
          `gas` string,
          `gas_price` string,
          `input` string
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'FALSE',
          'dots.in.keys' = 'FALSE',
          'case.insensitive' = 'TRUE',
          'mapping' = 'TRUE'
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{self.bucket}/{self.database}/transactions/'
        TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_log_table(self):
        query = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`logs` (
              `address` string,
              `log_index` bigint,
              `data` string,
              `block_number` bigint,
              `transaction_index` bigint,
              `block_hash` string,
              `topics` array < string >,
              `transaction_hash` string
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              'ignore.malformed.json' = 'FALSE',
              'dots.in.keys' = 'FALSE',
              'case.insensitive' = 'TRUE',
              'mapping' = 'TRUE'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{self.bucket}/{self.database}/logs/'
            TBLPROPERTIES ('classification' = 'json');
            '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    # def create_trace_column(self, database):
    #     pass

    def get_log_data(self, execution_id):
        data = self.athena_client.get_query_results(
            QueryExecutionId=execution_id
        )
        data = data['ResultSet']['Rows'][1:]
        result = []
        for log in data:
            item = log['Data']
            result.append({
                Log.address: item[0]['VarCharValue'],
                Log.log_index: int(item[1]['VarCharValue']),
                Log.data: item[2]['VarCharValue'],
                Log.block_number: int(item[3]['VarCharValue']),
                Log.transaction_index: int(item[4]['VarCharValue']),
                Log.block_hash: item[5]['VarCharValue'],
                Log.topics: item[6]['VarCharValue'][1:-1].split(","),
                Log.transaction_hash: item[7]['VarCharValue'],
            })

        return result

    def get_lp_token_data(self, execution_id):
        data = self.athena_client.get_query_results(
            QueryExecutionId=execution_id
        )
        data = data['ResultSet']['Rows'][1:]
        result = []
        for token in data:
            item = token['Data']
            result.append({
                "lp_token": item[0]['VarCharValue'],
                "token_a": item[1]['VarCharValue'],
                "token_b": item[2]['VarCharValue'],
                "pid": int(item[3]['VarCharValue']),
            })

        return result

    def get_holder_data(self, execution_id):
        data = self.athena_client.get_query_results(
            QueryExecutionId=execution_id
        )
        data = data['ResultSet']['Rows'][1:]
        result = []
        for token in data:
            item = token['Data']
            result.append({
                "lp_token": item[0]['VarCharValue'],
                "address": item[1]['VarCharValue'],
            })

        return result

    def get_lp_token_by_pid(self, database, start, end):
        query = f'''
        select * from {database}.lp_tokens
        where pid >= {start} and pid <={end}
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def get_lp_token_by_addresses(self, database, addresses):
        query = f'''
        select * from {database}.lp_tokens
        where lp_token in {tuple(addresses)}
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def get_lp_token_holder_by_addresses(self, database, addresses):
        query = f'''
        select * from {database}.holders
        where address in {tuple(addresses)}
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def get_lp_token_holder(self, database, address):
        query = f'''
        select * from {database}.holders
        where address = '{address}'
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def get_log_by_block_number_and_hash(self, database, start, end, hash):
        query = f'''
        select * from {database}.logs
        where block_number > {start} and block_number < {end} and topics[1] = '{hash}'
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)

        return response["QueryExecutionId"]

    def has_query_succeeded(self, execution_id):
        state = "RUNNING"
        max_execution = 5

        while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
            max_execution -= 1
            response = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            if (
                    "QueryExecution" in response
                    and "Status" in response["QueryExecution"]
                    and "State" in response["QueryExecution"]["Status"]
            ):
                state = response["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    return True

            time.sleep(30)

        return False

    def close(self):
        pass

    def open(self):
        pass


if __name__ == "__main__":
    # s3@AKIAXS2SFBSOTZFERP5O@GNHZ5JvbSoz5L8gjEvEbC0BRfk/XzuE7aHYLuycs@bangbich123@us-east-1
    job = AthenaS3StreamingExporter("AKIAXS2SFBSOTZFERP5O", "GNHZ5JvbSoz5L8gjEvEbC0BRfk/XzuE7aHYLuycs", "bangbich123",
                                    "onus", "us-east-1")
    m = job.get_log_by_block_number_and_hash("onus", 0, 5000000,
                                             "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
    while not job.has_query_succeeded(m):
        break

    data = job.get_log_data(m)

    print(data)
