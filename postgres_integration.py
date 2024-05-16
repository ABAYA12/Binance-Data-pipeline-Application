import psycopg2
import boto3
from io import BytesIO


def persist_to_postgres(bucket_name, file_name, host, database, user, password, port):
    """
    Persists data from an S3 bucket to PostgreSQL.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file in the S3 bucket.
        host (str): Host address of the PostgreSQL server.
        database (str): Name of the database.
        user (str): Username for authentication.
        password (str): Password for authentication.
        port (int): Port number of the PostgreSQL server.
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(host=host, database=database,
                                user=user, password=password, port=port)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS history_data (
                id SERIAL PRIMARY KEY,
                pair VARCHAR(50),
                time_spot TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                data BYTEA
            )
        """)

        # Fetch data from S3
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = response['Body'].read()

        # Insert data into PostgreSQL
        cursor.execute("INSERT INTO history_data (data) VALUES (%s)",
                       (psycopg2.Binary(data),))
        conn.commit()
        print("Data inserted into PostgreSQL successfully.")

    except psycopg2.Error as e:
        print(f"Error inserting data into PostgreSQL: {e}")
    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close cursor and connection
        if 'conn' in locals():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    pass  # Add main code logic here if needed
