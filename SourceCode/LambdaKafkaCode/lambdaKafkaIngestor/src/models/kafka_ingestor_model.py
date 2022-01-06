from src.classes.kinesis_class import KinesisManagement
from src.classes.kafka_class import KafkaManagement
from src.classes.secrets_class import SecretsManagement
from src.classes.s3_class import S3Management
from src.classes.dynamodb_class import DynamoDBManagement
from src.libs.datetime_lib import get_current_datetime
from src.errors.lambda_error import LambdaError
from time import sleep


def main(config, logger):
    """
    Principal function to kafka ingestor model. Orchestrates the flow between
    dynamodb , secrets manager, kinesis and kafka classes.
    :param config : List with configurations
    :param logger: Logging
    :return: operation result
    """
    current_time = get_current_datetime()
    logger.info(f"Execution datetime: {current_time}")
    execute_ingestion = False
    key_name = config['dynamodb']["primary_key_name"]
    key_value = config['kafka']["topic_name"] + "_" + str(config['kafka']["partition"])
    dynamodb_handler = None
    if config['dynamodb']["use_control_table"] == "true":
        dynamodb_handler = DynamoDBManagement(config, logger)
        if dynamodb_handler.create_record_info(key_name, key_value):
            execute_ingestion = True
    else:
        execute_ingestion = True

    result = {}
    #sleep(70)
    if execute_ingestion:
        if config['kafka']["use_authentication"] == "true":
           if config['kafka']["use_truststore_only"] == "false":
               secrets_handler = SecretsManagement(config, logger)
               config = secrets_handler.create_certificates_from_secrets()
        s3_handler = S3Management(config, logger)
        config = s3_handler.create_truststore_from_s3()
        kinesis_handler = KinesisManagement(config, logger)
        kafka_handler = KafkaManagement(config, logger)
        records_to_send = kafka_handler.read_kafka_messages()

        if len(records_to_send) > 0:
            try:
                kinesis_handler.send_records_to_stream(records_to_send, 2)
                result = {
                    "execution_datetime": current_time,
                    "detail": "successful"
                }
            except LambdaError as e:
                logger.error("Error" + str(e))
                result = {
                    "execution_datetime": current_time,
                    "detail": "fail"
                }
        else:
            result = {
                "execution_datetime": current_time,
                "detail": "successful_no_records"
            }
        if dynamodb_handler is not None:
            dynamodb_handler.clean_table(key_name, key_value)
    else:
        result = {
            "execution_datetime": current_time,
            "detail": "successful_partition_read_another_lambda"
        }
    execute_ingestion = False
    return result
