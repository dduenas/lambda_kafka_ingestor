import src.models.kafka_ingestor_model as kafka_ingestor_model


def main(config, logger):
    """
    Kafka ingestor controller main. its responsibility is to invoke the model class
    and process the correct status code result.

    :param config: Lambda configurations
    :param logger: Logging to show messages
    :return operation result.
    """
    process_response = kafka_ingestor_model.main(config, logger)
    logger.info("process_response" + str(process_response))
    status = process_records(
        config["status_message_codes"],
        process_response["detail"]
    )
    result = {
        "statusCode": status["code"],
        "message": status["message"],
        **process_response
    }

    logger.info(f"Execution result: {result}")
    return result


def process_records(status_message_codes, data):
    """
    Get status code to data process.

    :param status_message_codes: dict with codes and messages
    :param data: dict with total, fails and success data
    :return: dict with status
    """

    if data == "successful":
        result = status_message_codes["fully_processed"]
    elif data == "successful_no_records":
        result = status_message_codes["not_found_records"]
    elif data == "fail":
        result = status_message_codes["completely_failed"]
    elif data == "successful_partition_read_another_lambda":
        result = status_message_codes["another_lambda_processing_records"]
    else:
        result = None

    return result



