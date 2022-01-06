from src.errors.lambda_error import LambdaError
import boto3
from botocore.exceptions import ClientError
import time
from datetime import datetime, timedelta


class DynamoDBManagement():
    """Manage dynamodb interactions."""

    def __init__(self, config, logger):
        """
        Constructor method. handles all config properties for class and set class properties.
        :param config: config properties from config file
        :param logger: object to send logs to clowdwatch logs.
        """
        self.logger = logger
        self.control_table_name = config["dynamodb"]["control_table_name"]
        self.time_to_live_time_mins = config["dynamodb"]["time_to_live_time_mins"]

        try:
            self.dynamodb_client = boto3.resource('dynamodb',
                                                  region_name=config['aws_region_name'])
            self.table = self.dynamodb_client.Table(self.control_table_name)
        except (ClientError, KeyError) as error:
            raise LambdaError(
                f"Failed on [dynamodb_class][__init__]. Detail: {error}")

    def create_record_info(self, key_name, key_value):
        """
        Create a record in dynamodb table, given a key name and value. Adds a timestamp and a ttl
        field.

        :param key_name: name of the primary key in the dynamodb table
        :param key_value: value of the primary key to insert in the table
        :return: True if the record could be created.
                 False if the record exists
        """
        try:
            response = self.table.get_item(Key={key_name: key_value})
            table_item = response.get('Item', None)
            if table_item:
                if(self.record_is_expired(table_item)):
                    self.clean_table(key_name, key_value)
                    self.logger.info("Cleaning table because ttl expired..")
                return False
            else:
                ttl = int(time.time()) + (int(self.time_to_live_time_mins) * 60)
                dynamodb_record = {
                    key_name: key_value,
                    'ttl': ttl,
                    'timestamp': time.strftime("%Y-%m-%dT%H:%M:%S-%Z")
                }
                response = self.table.put_item(Item=dynamodb_record)
                self.logger.info("Inserting in DynamoDB table {} with result: {}"
                                 .format(self.control_table_name, response))
                return True
        except (ClientError, KeyError) as error:
            raise LambdaError("Failed on [dynamodb_class][create_record_info]."
                              f"\nDetail: {error}")

    def clean_table(self, key_name, key_value):
        """
        delets a record in a dynamodb table given a primary key and a value

        :param key_name: name of the primary key in the dynamodb table
        :param key_value: value of the primary key to delete in the table
        :return: resulting http response code
        """
        self.logger.info(
            "Cleaning Table DynamoDB: {}".format(self.control_table_name))
        try:
            response = self.table.delete_item(Key={key_name: key_value})
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            self.logger.info("HTTPStatusCode: {}".format(response_code))
            return response_code
        except (ClientError, KeyError) as error:
            raise LambdaError("Failed on delete [dynamodb_class][clean_table]."
                              f"\nDetail: {error}")

    def record_is_expired(self, dynamodb_record):
        expiration_date_milis = dynamodb_record["ttl"]
        self.logger.info("Expiration Milis: {}".format(expiration_date_milis))
        expired_datetime = datetime.utcfromtimestamp(int(expiration_date_milis))
        datetime_now = datetime.now()
        self.logger.info("Datetime Now: {}".format(datetime_now))
        self.logger.info("Expired Time: {}".format(expired_datetime))
        if (datetime_now > expired_datetime):
            return True
        else:
            return False


