from src.controllers import kafka_ingestor_controller, health_controller
import logging
import json


def get_configurations():
    """
    Load JSON with configurations.
    :return: List with configuration
    """
    with open("config/config-dev.json", "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def handler(event, context):
    """
    Event handler for this lambda. This method needs to be configured as the entry point
    in the lambda configuration.

    If you require to work with a certificate authority (ca) file in config folder, you must
    specify such location under the "ssl_ca_location" key in "kafka" section of config.json file.
    For example: "config/all.pem" can be a acceptable value. Otherwise, point to the proper secret
    name where lambda can retrieve the ca value and store it locally for kafka config. Use the
    "secrets_ssl_ca_location" key instead for such scenario.

    :param event: Dict with event data. In this case, the event will have the form:
        {
          "kafka_partition": 0,
          "iteration": 1
        }
    indicating the kafka partition number where the lambda will read new messages.
    :param context: Dict with context from AWS
    """
    config = get_configurations()
    config["kafka"]["partition"] = event.get("kafka_partition")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("Iteration Number::" + str(event.get("iteration")))
    logger.info("kafka_partition_number::" + str(config["kafka"]["partition"]))
    if event and event.get("type", "") == config["events"]["health"]:
        return health_controller.main()
    else:
        return kafka_ingestor_controller.main(config, logger)


if __name__ == '__main__':
    print(handler({}, {}))
