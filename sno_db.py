# binance_data.py
from postgres_integration import persist_to_postgres
from snowflake_integration import persist_to_snowflake
from s3_integration import upload_to_s3_and_grant_permissions
from binance_data import initialize_binance_client, get_historical_klines, timestamp_to_datetime
import pandas as pd
from io import BytesIO
import zipfile
import snowflake.connector
from io import StringIO
import boto3
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


# s3_integration.py

AWS_KEY_ID = 'AKIAZQ3DQVW5HEBI465G'
AWS_SECRET_ACCESS_KEY = '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'


def upload_to_s3_and_grant_permissions(data, bucket_name, file_name):
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)

    # Generate a presigned URL with public-read access
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': file_name},
        ExpiresIn=3600,  # URL expiration time (1 hour)
        HttpMethod='GET',  # Allow GET requests
    )

    return url


def persist_to_snowflake(bucket_name, file_name, user, password, account, warehouse, database, role):
    try:
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            role=role
        )
        cursor = ctx.cursor()

        # Create a temporary stage in Snowflake with a qualified name
        stage_name = f"{database}.public.tmp_stage_{
            file_name.replace('.', '_')}"
        cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")

        # Download data from S3
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = response['Body'].read()

        # Upload data to the Snowflake stage
        with BytesIO(data) as data_stream:
            cursor.execute(
                f"PUT 's3://{bucket_name}/{file_name}' @{stage_name}")

        # Copy data from the stage into Snowflake table
        cursor.execute(
            f"COPY INTO history_data FROM @{stage_name}/{file_name} FILE_FORMAT=(TYPE='CSV')")

        # Commit transaction
        ctx.commit()
        print("Data uploaded to Snowflake successfully.")

    except Exception as e:
        print(f"Error uploading data to Snowflake: {e}")

    finally:
        # Close cursor and connection
        cursor.close()
        ctx.close()


# main.py


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
    print(f'CLICK ON LINK BELOW TO GET DATA IN ".csv"\n--->>> {url}')

    # Persist data to Snowflake data warehouse
    User = 'SLYNOS'
    Password = 'Cloud1ngineering'
    Account = 'qeb24678.us-east-1'
    Database = 'BINANCE_DATA'
    Warehouse = 'BINANCE'
    Role = 'ACCOUNTADMIN'
    persist_to_snowflake(csv_data, user=User, password=Password,
                         account=Account, database=Database,
                         warehouse=Warehouse, role=Role, file_name=Role)

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
