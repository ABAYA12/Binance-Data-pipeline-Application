# # pip install python-binance tqdm pandas boto3 psycopg2 snowflake-connector-python

# # Import necessary libraries
# from binance.client import Client  # Importing Binance API client
# from tqdm import tqdm  # Importing tqdm for progress bars
# import pandas as pd  # Importing pandas for data manipulation
# import datetime  # Importing datetime for handling date and time
# import zipfile
# import boto3  # Importing Boto3 for AWS S3 integration
# from io import StringIO  # Importing StringIO for file handling
# import psycopg2  # Importing psycopg2 for PostgreSQL integration
# import snowflake.connector  # Importing snowflake connector for Snowflake integration

# # Binance API credentials
# API_KEY = "IRmKve67nP3ewFyGSdSAs5RKTrHjgjJ5BLu6gCiX2y1dbhUqqohSTvMcI4Qosuid"
# API_SECRET = "sRVPKcsjYqpLNMrtcqkIilnbEcBDBuCif7xY8833deQDVZd9HoysyUaIEhD66juJ"

# # AWS credentials
# AWS_KEY_ID = 'AKIAZQ3DQVW5HEBI465G'
# AWS_SECRET_ACCESS_KEY = 'uGJ1LUFKaCqF4RaHyMgUvB7Skj9FqPQXMRJ8lAfP'

# # Snowflake credentials
# SNOWFLAKE_USER = 'Slynos'
# SNOWFLAKE_PASSWORD = 'YOUR_SNOWFLAKE_PASSWORD'
# SNOWFLAKE_ACCOUNT = 'YOUR_SNOWFLAKE_ACCOUNT'
# SNOWFLAKE_DATABASE = 'YOUR_SNOWFLAKE_DATABASE'
# SNOWFLAKE_WAREHOUSE = 'YOUR_SNOWFLAKE_WAREHOUSE'
# SNOWFLAKE_ROLE = 'YOUR_SNOWFLAKE_ROLE'

# # PostgreSQL credentials
# POSTGRES_HOST = 'YOUR_POSTGRES_HOST'
# POSTGRES_DB = 'YOUR_POSTGRES_DB'
# POSTGRES_USER = 'YOUR_POSTGRES_USER'
# POSTGRES_PASSWORD = 'YOUR_POSTGRES_PASSWORD'
# POSTGRES_PORT = 'YOUR_POSTGRES_PORT'

# # Initialize Binance API client


# def initialize_binance_client():
#     return Client(API_KEY, API_SECRET)

# # Fetch historical kline data for specified pairs


# def get_historical_klines(client, pairs, start_date, end_date):
#     historical_data = {}
#     for pair in pairs:
#         try:
#             # Fetch historical data with 4-hour candlestick interval
#             history_data = client.get_historical_klines(
#                 pair, Client.KLINE_INTERVAL_4HOUR, start_date, end_date)
#             historical_data[pair] = history_data
#         except Exception as e:
#             print(f"Failed to fetch data for {pair}: {e}")
#     return historical_data

# # Convert timestamp to datetime object


# def timestamp_to_datetime(timestamp):
#     return datetime.datetime.fromtimestamp(timestamp / 1000)

# # Model the fetched data into DataFrame


# def model_data(historical_data):
#     Time_spot, Open, High, Low, Close, Volume, Pair = [], [], [], [], [], [], []
#     for pair, history_data in historical_data.items():
#         if history_data:
#             for num in tqdm(range(0, len(history_data))):
#                 Time_spot.append(timestamp_to_datetime(history_data[num][0]))
#                 Open.append(history_data[num][1])
#                 High.append(history_data[num][2])
#                 Low.append(history_data[num][3])
#                 Close.append(history_data[num][4])
#                 Volume.append(history_data[num][5])
#                 Pair.append(pair)
#         else:
#             print(f"No data retrieved for {pair}")
#     df = pd.DataFrame({
#         'Pair': Pair,
#         'Time_spot': Time_spot,
#         'Open': Open,
#         'High': High,
#         'Low': Low,
#         'Close': Close,
#         'Volume': Volume,
#     })
#     df.sort_values(by='Time_spot', ascending=False, inplace=True)
#     return df

# # Save DataFrame to CSV format


# def save_to_csv(df, file_name):
#     csv_buffer = StringIO()
#     df.to_csv(csv_buffer, index=False)
#     return csv_buffer.getvalue()

# # Upload CSV data to S3 bucket and grant permissions to all members


# def upload_to_s3_and_grant_permissions(data, bucket_name, file_name):
#     s3 = boto3.client('s3', aws_access_key_id=AWS_KEY_ID,
#                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     s3.create_bucket(Bucket=bucket_name)
#     s3.put_object(Body=data, Bucket=bucket_name,
#                   Key=file_name, ACL='public-read')
#     # Grant public access to the bucket
#     s3.put_bucket_acl(ACL='public-read', Bucket=bucket_name)
#     url = s3.generate_presigned_url(
#         'get_object', Params={'Bucket': bucket_name, 'Key': file_name}, ExpiresIn=3600)
#     return url

# # Persist data to Snowflake data warehouse


# def persist_to_snowflake(data, user, password, account, database, warehouse, role, file_name):
#     ctx = snowflake.connector.connect(
#         user=user,
#         password=password,
#         account=account,
#         warehouse=warehouse,
#         database=database,
#         role=role
#     )
#     cursor = ctx.cursor()
#     with zipfile.ZipFile('history_data.zip', 'w') as zip_file:
#         zip_file.writestr(file_name, data)
#     with open('history_data.zip', 'rb') as f:
#         cursor.execute(
#             "CREATE TABLE IF NOT EXISTS history_data (data VARIANT)")
#         cursor.execute("PUT file://" + 'history_data.zip' + " @%history_data")
#         cursor.execute(
#             "COPY INTO history_data from @%history_data FILE_FORMAT=(TYPE= 'csv')")
#     cursor.close()
#     ctx.close()

# # Persist data to PostgreSQL database


# def persist_to_postgres(data, host, database, user, password, port, file_name):
#     conn = psycopg2.connect(host=host, database=database,
#                             user=user, password=password, port=port)
#     cursor = conn.cursor()
#     cursor.execute("CREATE TABLE IF NOT EXISTS history_data (data BYTEA)")
#     with open('history_data.zip', 'rb') as f:
#         cursor.execute("INSERT INTO history_data (data) VALUES (%s)",
#                        (psycopg2.Binary(f.read()),))
#     conn.commit()
#     cursor.close()
#     conn.close()

# # Main function


# def main():
#     start_time = datetime.datetime.now()

#     # Initialize Binance API client
#     client = initialize_binance_client()

#     # Define pairs and time range
#     pairs = ["BTCUSDT", "BNBUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT",
#              "ADAUSDT", "DOTUSDT", "SOLUSDT", "DOGEUSDT", "AVAXUSDT"]
#     start_date = "2024-01-01"
#     end_date = "2024-12-31"

#     # Fetch historical kline data
#     historical_data = get_historical_klines(
#         client, pairs, start_date, end_date)

#     # Model the fetched data
#     df = model_data(historical_data)

#     # Save DataFrame to CSV
#     csv_data = save_to_csv(df, 'history_data.csv')

#     # Upload CSV data to S3 and grant permissions
#     bucket_name = 'binance_2024_data'
#     file_name = 'history_data.csv'
#     url = upload_to_s3_and_grant_permissions(csv_data, bucket_name, file_name)

#     # Persist data to Snowflake data warehouse
#     persist_to_snowflake(csv_data, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
#                          SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, file_name)

#     # Persist data to PostgreSQL database
#     persist_to_postgres(csv_data, POSTGRES_HOST, POSTGRES_DB,
#                         POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT, file_name)

#     # Print script execution time
#     print("Run time:", datetime.datetime.now() - start_time)


# # Execute the main function
# if __name__ == "__main__":
#     main()
