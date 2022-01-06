from datetime import datetime


def main():
    """
    Principal function to get health.

    :return: dict with status
    """
    return {
        "statusCode": 200,
        "message": "ALIVE",
        "timestamp": str(datetime.now())
    }
