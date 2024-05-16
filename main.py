# main.py
import datetime
import pandas as pd
from io import StringIO
from tqdm import tqdm
from binance_data import initialize_binance_client, get_historical_klines, timestamp_to_datetime
from s3_integration import upload_to_s3_and_grant_permissions
from snowflake_integration import persist_to_snowflake
from postgres_integration import persist_to_postgres


def model_data(historical_data):
    Time_spot, Open, High, Low, Close, Volume, Pair = [], [], [], [], [], [], []
    for pair, history_data in historical_data.items():
        if history_data:
            for num in tqdm(range(0, len(history_data))):
                Time_spot.append(timestamp_to_datetime(history_data[num][0]))
                Open.append(history_data[num][1])
                High.append(history_data[num][2])
                Low.append(history_data[num][3])
                Close.append(history_data[num][4])
                Volume.append(history_data[num][5])
                Pair.append(pair)
        else:
            print(f"No data retrieved for {pair}")
    df = pd.DataFrame({
        'Pair': Pair,
        'Time_spot': Time_spot,
        'Open': Open,
        'High': High,
        'Low': Low,
        'Close': Close,
        'Volume': Volume,
    })
    df.sort_values(by='Time_spot', ascending=False, inplace=True)
    return df


def save_to_csv(df, file_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def main():
    start_time = datetime.datetime.now()

    # Initialize Binance API client
    client = initialize_binance_client()

    # Define pairs and time range
    pairs = ["BTCUSDT", "BNBUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT",
             "ADAUSDT", "DOTUSDT", "SOLUSDT", "DOGEUSDT", "AVAXUSDT"]
    start_date = "2024-01-01"
    end_date = "2024-12-31"

    # Fetch historical kline data
    historical_data = get_historical_klines(
        client, pairs, start_date, end_date)

    # Model the fetched data
    df = model_data(historical_data)

    # Save DataFrame to CSV
    csv_data = save_to_csv(df, 'history_data.csv')

    # Upload CSV data to S3 and grant permissions
    bucket_name = 'binance-trestle-data-2024'
    file_name = 'history_data.csv'
    url = upload_to_s3_and_grant_permissions(csv_data, bucket_name, file_name)
    print()
    print(f'CLICK ON LINK BELOW TO GET DATA IN ".csv"\n--->>> {url}')

    # Persist data to Snowflake data warehouse
    User = 'SLYNOS'
    Password = 'Cloud1ngineering'
    Account = 'qeb24678.us-east-1'
    Database = 'BINANCE_DATA'
    Warehouse = 'BINANCE'
    Role = 'ACCOUNTADMIN'
    # fileName = file_name

    persist_to_snowflake(csv_data, user=User, password=Password,
                         account=Account, database=Database,
                         warehouse=Warehouse, role=Role, file_name=file_name)

    # Persist data to PostgreSQL database
    Host = 'localhost'
    Database = 'Binance_DB'
    User = 'postgres'
    Password = 'post123'

    persist_to_postgres(csv_data, host=Host, database=Database,
                        user=User, password=Password,
                        port=5432, file_name=file_name)

    # Print script execution time
    print("Run time:", datetime.datetime.now() - start_time)


if __name__ == "__main__":
    main()
