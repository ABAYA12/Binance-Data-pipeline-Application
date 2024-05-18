
"""
Module for retrieving historical data from Binance.
"""

import datetime
from binance.client import Client

API_KEY = "IRmKve67nP3ewFyGSdSAs5RKTrHjgjJ5BLu6gCiX2y1dbhUqqohSTvMcI4Qosuid"
API_SECRET = "sRVPKcsjYqpLNMrtcqkIilnbEcBDBuCif7xY8833deQDVZd9HoysyUaIEhD66juJ"


def initialize_binance_client():
    """
    Initializes the Binance client.
    """
    return Client(API_KEY, API_SECRET)


def get_historical_klines(client, pairs, start_date, end_date):
    """
    Retrieves historical klines for the specified pairs and time period.

    Args:
        client: Binance client instance.
        pairs (list): List of trading pairs.
        start_date (datetime.datetime): Start date for historical data.
        end_date (datetime.datetime): End date for historical data.

    Returns:
        dict: Historical data for each pair.
    """
    historical_data = {}
    for pair in pairs:
        try:
            history_data = client.get_historical_klines(
                pair, Client.KLINE_INTERVAL_4HOUR, start_date, end_date)
            historical_data[pair] = history_data
        except Exception as e:  # pylint: disable=broad-except
            print(f"Failed to fetch data for {pair}: {e}")
    return historical_data


def timestamp_to_datetime(timestamp):
    """
    Converts a timestamp to a datetime object.

    Args:
        timestamp (int): Unix timestamp.

    Returns:
        datetime.datetime: Datetime object.
    """
    return datetime.datetime.fromtimestamp(timestamp / 1000)


if __name__ == "__main__":
    pass  # any logic can be added here by removing the pass statement
