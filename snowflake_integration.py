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

        # Create a temporary stage in Snowflake with a qualified name
        stage_name = f"""{database}.{schema}.tmp_stage_{
            file_name.replace('.', '_')}"""
        cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")

        # Copy data from the zip file in the stage into Snowflake table
        cursor.execute(
            f"COPY INTO {schema}.history_data FROM '@{stage_name}/{file_name}.zip' FILE_FORMAT=(TYPE='CSV', FIELD_OPTIONALLY_ENCLOSED_BY='\"', SKIP_HEADER=1)")

        # Commit transaction
        ctx.commit()
        print("Data uploaded to Snowflake successfully.")

    except Exception as e:
        print(f"Error uploading data to Snowflake: {e}")

    finally:
        # Close cursor and connection
        cursor.close()
        ctx.close()
