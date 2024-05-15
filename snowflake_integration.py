"""
Module for integrating with Snowflake.
"""

import zipfile
from io import BytesIO
import snowflake.connector
import boto3


def persist_to_snowflake(bucket_name, file_name, user, password, account, warehouse, database, role):
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
            role=role
        )
        cursor = ctx.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS history_data (
                Pair VARCHAR(50),
                Time_spot TIMESTAMP,
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume FLOAT
            )
        """)

        # Create a temporary stage in Snowflake with a qualified name
        stage_name = f"""{database}.public.tmp_stage_{
            file_name.replace('.', '_')}"""
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


# import boto3
# import snowflake.connector
# import zipfile
# from io import BytesIO


# def persist_to_snowflake(bucket_name, file_name, user, password, account, warehouse, database, role):
#     try:
#         # Connect to Snowflake
#         ctx = snowflake.connector.connect(
#             user=user,
#             password=password,
#             account=account,
#             warehouse=warehouse,
#             database=database,
#             role=role
#         )
#         cursor = ctx.cursor()

#         # Create a temporary stage in Snowflake with a qualified name
#         stage_name = f"{database}.public.tmp_stage_{
#             file_name.replace('.', '_')}"
#         cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")

#         # Download data from S3
#         s3 = boto3.client('s3')
#         response = s3.get_object(Bucket=bucket_name, Key=file_name)
#         data = response['Body'].read()

#         # Upload data to the Snowflake stage
#         with BytesIO(data) as data_stream:
#             cursor.execute(
#                 f"PUT 's3://{bucket_name}/{file_name}' @{stage_name}")

#         # Copy data from the stage into Snowflake table
#         cursor.execute(
#             f"COPY INTO history_data FROM @{stage_name}/{file_name} FILE_FORMAT=(TYPE='CSV')")

#         # Commit transaction
#         ctx.commit()
#         print("Data uploaded to Snowflake successfully.")

#     except Exception as e:
#         print(f"Error uploading data to Snowflake: {e}")

#     finally:
#         # Close cursor and connection
#         cursor.close()
#         ctx.close()
