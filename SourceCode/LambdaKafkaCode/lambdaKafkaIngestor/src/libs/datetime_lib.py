from datetime import datetime, timezone, timedelta


def get_current_datetime():
    """
    Get datetime in GMT-5 (Colombia) timezone with format YYYY-mm-dd HH:00:00.
    :return: String with current datetime
    """
    timezone_co = timezone(timedelta(hours=-5))
    current_datetime = datetime.now(timezone_co)
    return current_datetime.strftime("%Y-%m-%d %H:00:00")
