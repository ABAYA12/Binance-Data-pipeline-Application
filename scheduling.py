import datetime
import pandas as pd
from io import StringIO
from tqdm import tqdm
from binance_data import initialize_binance_client, get_historical_klines, timestamp_to_datetime
from s3_integration import upload_to_s3_and_grant_permissions
from snowflake_integration import persist_to_snowflake
from postgres_integration import persist_to_postgres
import luigi

class ModelData(luigi.Task):
    def output(self):
        return luigi.LocalTarget("history_data.csv")
    
    def run(self):
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
        
        with self.output().open('w') as f:
            f.write(csv_data)
        
        print("ModelData Task execution time:", datetime.datetime.now() - start_time)

class UploadToS3(luigi.Task):
    def requires(self):
        return ModelData()

    def output(self):
        return luigi.LocalTarget("s3_upload.txt")
    
    def run(self):
        with self.input().open('r') as f:
            csv_data = f.read()
        url = upload_to_s3_and_grant_permissions(csv_data, 'binance-trestle-data-2024', 'history_data.csv')
        
        with self.output().open('w') as f:
            f.write(url)

class PersistToSnowflake(luigi.Task):
    def requires(self):
        return UploadToS3()
    
    def run(self):
        with self.input().open('r') as f:
            url = f.read()
        persist_to_snowflake(url)

class PersistToPostgres(luigi.Task):
    def requires(self):
        return UploadToS3()
    
    def run(self):
        with self.input().open('r') as f:
            url = f.read()
        persist_to_postgres(url)

if __name__ == '__main__':
    luigi.build([PersistToSnowflake(), PersistToPostgres()], local_scheduler=True)
