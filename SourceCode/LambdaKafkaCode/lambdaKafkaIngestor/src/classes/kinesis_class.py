from src.errors.lambda_error import LambdaError
import boto3
from botocore.exceptions import ClientError
import time


class KinesisManagement():
    """Manage kinesis datastream interactions."""

    def __init__(self, config, logger):
        """
        Constructor method. handles all config properties for class and set class properties.
        :param config: config properties from config file
        :param logger: object to send logs to clowdwatch logs.
        """
        self.logger = logger
        self.retry_count = config["kinesis"]["retry_count"]
        self.retry_wait_in_sec = config["kinesis"]["retry_wait_in_sec"]
        self.stream_name = config["kinesis"]["stream_name"]

        try:
            self.kinesis_client = boto3.client('kinesis', region_name=config['aws_region_name'])
        except (ClientError, KeyError) as error:
            raise LambdaError(
                f"Failed on [kinesis_class][__init__]. Detail: {error}")

    def send_records_to_stream(self, kinesis_records, retry_count):
        """
        Send records to kinesis data stream with retry support.

        :param kinesis_records: array with the kinesis records to be sent to the stream
        :param retry_count: number of retries the code attempt before considering the sent failed
        :return: string with "success" or lambda exception raise depending on the result operation
        """
        try:
            put_response = self.kinesis_client.put_records(
                Records=kinesis_records,
                StreamName=self.stream_name
            )
            failed_count = put_response['FailedRecordCount']
            if failed_count > 0:
                if self.retry_count > 0:
                    self.logger.info("put_response: " + str(put_response))
                    self.logger.info("FailedRecordCount: " + str(failed_count))
                    self.logger.info("Retrying......")
                    retry_kinesis_records = []
                    for idx, record in enumerate(put_response['Records']):
                        if 'ErrorCode' in record:
                            retry_kinesis_records.append(kinesis_records[idx])
                    time.sleep(self.retry_wait_in_sec * (self.retry_count - self.retry_count + 1))
                    self.send_records_to_stream(retry_kinesis_records, retry_count - 1)
                else:
                    self.logger.info(f'Not able to put records after retries. Records '
                          f'= {put_response["Records"]}')
            else:
                self.logger.info("kinesis records processed successfully")
            return "success"

        except (ClientError, KeyError) as error:
            raise LambdaError("Failed on [kinesis_class][send_records_to_stream]."
                              f"\nDetail: {error}")
