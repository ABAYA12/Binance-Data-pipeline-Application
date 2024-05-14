# binance_data.py
from binance.client import Client
from tqdm import tqdm
import datetime

API_KEY = "IRmKve67nP3ewFyGSdSAs5RKTrHjgjJ5BLu6gCiX2y1dbhUqqohSTvMcI4Qosuid"
API_SECRET = "sRVPKcsjYqpLNMrtcqkIilnbEcBDBuCif7xY8833deQDVZd9HoysyUaIEhD66juJ"


def initialize_binance_client():
    return Client(API_KEY, API_SECRET)


def get_historical_klines(client, pairs, start_date, end_date):
    historical_data = {}
    for pair in pairs:
        try:
            history_data = client.get_historical_klines(
                pair, Client.KLINE_INTERVAL_4HOUR, start_date, end_date)
            historical_data[pair] = history_data
        except Exception as e:
            print(f"Failed to fetch data for {pair}: {e}")
    return historical_data


def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp / 1000)
