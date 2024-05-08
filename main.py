# pip install python-binance tqdm pandas boto3

# Import necessary libraries
from binance.client import Client  # Importing Binance API client
from tqdm import tqdm  # Importing tqdm for progress bars
import pandas as pd  # Importing pandas for data manipulation
import datetime  # Importing datetime for handling date and time
import boto3  # Importing Boto3 for AWS S3 integration
from io import StringIO  # Importing StringIO for file handling

# Function to fetch historical kline data for specified pairs


def example_get_historical_klines(client):
    # Define pairs to fetch historical data for
    pairs = ["BTCUSDT", "BNBUSDT", "ETHUSDT", "XRPUSDT",
             "LTCUSDT", "ADAUSDT"]  # Adding 3 more pairs
    start_date = "1 Jan, 2024"
    end_date = "31 Dec, 2024"

    historical_data = {}

    # Loop through pairs and fetch historical data
    for pair in pairs:
        try:
            # Fetch historical data with 8-hour candlestick interval
            history_data = client.get_historical_klines(
                pair, Client.KLINE_INTERVAL_4HOUR, start_date, end_date)
            historical_data[pair] = history_data
        except Exception as e:
            print(f"Failed to fetch data for {pair}: {e}")

    return historical_data

# Function to convert timestamp to datetime object


def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp / 1000)

# Main function to execute the script


def main():
    start_time = datetime.datetime.now()

    # Binance API credentials
    API_key = "IRmKve67nP3ewFyGSdSAs5RKTrHjgjJ5BLu6gCiX2y1dbhUqqohSTvMcI4Qosuid"
    API_Secret_key = "sRVPKcsjYqpLNMrtcqkIilnbEcBDBuCif7xY8833deQDVZd9HoysyUaIEhD66juJ"

    # Initialize Binance API client
    client = Client(API_key, API_Secret_key)

    # Fetch historical kline data
    historical_data = example_get_historical_klines(client)

    # Initialize lists to store data
    Time_spot = []
    Open = []
    High = []
    Low = []
    Close = []
    Volume = []
    Pair = []

    # Loop through historical data and extract required information
    for pair, history_data in historical_data.items():
        if history_data:  # Check if historical data is retrieved successfully
            for num in tqdm(range(0, len(history_data))):
                Time_spot.append(timestamp_to_datetime(history_data[num][0]))
                Open.append(history_data[num][1])
                High.append(history_data[num][2])
                Low.append(history_data[num][3])
                Close.append(history_data[num][4])
                Volume.append(history_data[num][5])
                Pair.append(pair)  # Store pair name for each data point
        else:
            print(f"No data retrieved for {pair}")

    # Create DataFrame from collected data
    df = pd.DataFrame({
        'Pair': Pair,
        'Time_spot': Time_spot,
        'Open': Open,
        'High': High,
        'Low': Low,
        'Close': Close,
        'Volume': Volume,
    })

    # Sort DataFrame by 'Time_spot' column in descending order
    df.sort_values(by='Time_spot', ascending=False, inplace=True)

    # Save DataFrame to CSV file in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload CSV data to S3 bucket
    s3 = boto3.client('s3',
                      aws_access_key_id='YOUR_ACCESS_KEY_ID',
                      aws_secret_access_key='YOUR_SECRET_ACCESS_KEY')

    bucket_name = 'your-s3-bucket-name'
    file_name = 'history_data.csv'

    s3.put_object(Bucket=bucket_name, Key=file_name,
                  Body=csv_buffer.getvalue())

    # Print script execution time
    print("Run time : ", datetime.datetime.now() - start_time)


# Execute the main function
main()
