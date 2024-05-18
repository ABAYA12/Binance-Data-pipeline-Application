
import zipfile
from io import BytesIO
import snowflake.connector
import boto3


def persist_to_snowflake(bucket_name, file_name, user, password, account, warehouse, database, role, schema='PUBLIC'):
    """
    Persist data to Snowflake.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file in the S3 bucket.
        user (str): Snowflake user.
        password (str): Snowflake password.
        account (str): Snowflake account.
        warehouse (str): Snowflake warehouse.
        database (str): Snowflake database.
        schema (str): Snowflake schema.
        role (str): Snowflake role.
    """
    try:
        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            role=role,
        )
        cursor = ctx.cursor()

        # Create table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.history_data (
                Pair VARCHAR(50),
                Time_spot TIMESTAMP,
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume FLOAT
            )
        """)

        # Initialize S3 client
        s3 = boto3.client('s3')

        # Get the zip file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        zip_file_bytes = response['Body'].read()

        # Read the zip file content
        with zipfile.ZipFile(BytesIO(zip_file_bytes)) as zip_file:
            for contained_file in zip_file.namelist():
                with zip_file.open(contained_file) as file:
                    # Load data into Snowflake from the extracted file
                    cursor.execute(
                        f"PUT file://{file.name} @{stage_name}/{contained_file}")
                    cursor.execute(
                        f"""
                        COPY INTO {schema}.history_data
                        FROM '@{stage_name}/{contained_file}'
                        FILE_FORMAT=(TYPE='CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
                        """
                    )

        # Commit transaction
        ctx.commit()
        print("Data uploaded to Snowflake successfully.")

    except Exception as e:
        print(f"Error uploading data to Snowflake: {e}")

    finally:
        # Close cursor and connection
        cursor.close()
        ctx.close()


if __name__ == "__main__":
    pass   # any logic can be added here by removing the pass statement
