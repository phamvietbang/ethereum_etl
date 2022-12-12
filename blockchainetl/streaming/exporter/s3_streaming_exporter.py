import json
import boto3


class S3StreamingExporter(object):
    def __init__(self, access_key, secret_key, bucket, aws_region=None):
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

    # def export_items(self, chain, folder, data):
    #     for data_dict in data:
    #         data_string = json.dumps(data_dict, indent=2, default=str)
    #         if folder == "transactions":
    #             self.s3_client.put_object(
    #                 Bucket=self.bucket,
    #                 Key=f'{chain}/{folder}/{data_dict["hash"]}',
    #                 Body=data_string
    #             )
    #         if folder == "logs":
    #             self.s3_client.put_object(
    #                 Bucket=self.bucket,
    #                 Key=f'{chain}/{folder}/{data_dict["block_number"]}_{data_dict["log_index"]}',
    #                 Body=data_string
    #             )

    def export_items(self, chain, folder, data, file_name=None):
        # for data_dict in data:
        if not data:
            return
        data_string = json.dumps(data, indent=2, default=str)
        self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=f'{chain}/{folder}/{file_name}',
                    Body=data_string
                )

    def close(self):
        pass

    def open(self):
        pass
# s3@AKIAXS2SFBSOTZFERP5O@GNHZ5JvbSoz5L8gjEvEbC0BRfk/XzuE7aHYLuycs@bangbich123@us-east-1