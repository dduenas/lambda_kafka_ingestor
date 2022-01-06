from botocore.exceptions import ClientError
from src.errors.lambda_error import LambdaError
from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException
import random


class KafkaManagement():
    """Class to manage kafka ingestion"""

    def __init__(self, config, logger):
        """
        Constructor method. handles all config properties for class and set class properties.
        :param config: config properties from config file
        :param logger: object to send logs to clowdwatch logs.
        """
        self.logger = logger

        try:
            self.topic = config['kafka']["topic_name"]
            self.group_id = config['kafka']["group_id"]
            self.batch_size = config['kafka']["records_batch_size"]
            self.poll_seconds = config['kafka']["poll_seconds"]
            self.kafka_partition = config['kafka']["partition"]
            self.consumer_config = {
                'bootstrap.servers': config['kafka']["bootstrap_servers"],
                'group.id': config['kafka']["group_id"],
                'enable.auto.commit': config['kafka']["enable_auto_commit"],
                'security.protocol': config['kafka']["security_protocol"]
            }
            if config['kafka']["use_authentication"] == "true":
                self.consumer_config["ssl.ca.location"] = config['kafka']["ssl_ca_location"]
                if config['kafka']["use_truststore_only"] == "false":
                    self.consumer_config["ssl.certificate.location"] \
                        = config['kafka']["ssl_certificate_location"]
                    self.consumer_config["ssl.key.location"] = config['kafka']["ssl_key_location"]

            logger.info("Consumer Config:" + str(self.consumer_config))
            self.consumer = Consumer(self.consumer_config)

        except (ClientError, KeyError) as error:
            raise LambdaError("Failed on [kafka_class][__init__].\n"
                              f"Detail: {error}")

    def read_kafka_messages(self):
        """
        read kafka messages from the defined topic and partition.
        in this case is used the line:
        self.consumer.assign([topic_partition])
        because is to consume for an specific partition.
        To consume for a topic with dynamic partition assignment use:
        self.consumer.subscribe([self.topic])
        :return array of messages read from kafka
        """
        try:
            topic_partition = TopicPartition(self.topic, self.kafka_partition)
            self.logger.info("Before partition assign..")
            self.consumer.assign([topic_partition])
            self.logger.info("After partition assign..")
            records = []
            for i in range(0, self.batch_size):
                msg = self.consumer.poll(self.poll_seconds)
                if msg is None:
                    self.logger.info("No more messages in queue in poll time")
                    break
                elif msg.error() is not None \
                        and msg.error().code() is not None \
                        and msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.info("End of messages in partition processed")
                    break
                else:
                    original_log = msg.value().decode("utf-8")
                    data = original_log.replace("\n", "") + "\n"
                    self.logger.debug("Offset:" + str(msg.offset()))
                    self.logger.debug("Partition:" + str(msg.partition()))
                    encode_data = data.encode('utf-8')
                    record = {"PartitionKey": str(random.randrange(100)), "Data": encode_data}
                    records.append(record)
            self.logger.info("before returning records..")
            return records
        except (KafkaException, KeyError, ClientError) as error:
            raise LambdaError(
                "Failed on [kafka_class][read_kafka_messages].\n"
                f"Detail: {error}")
        finally:
            self.logger.info("closing consumer")
            self.consumer.close()
            self.consumer_config = None
            self.consumer = None
