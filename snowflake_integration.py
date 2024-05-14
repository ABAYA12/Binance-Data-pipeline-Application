# snowflake_integration.py
import snowflake.connector
import zipfile


def persist_to_snowflake(data, user, password, account, database, warehouse, role, file_name):
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        role=role
    )
    cursor = ctx.cursor()
    with zipfile.ZipFile('history_data.zip', 'w') as zip_file:
        zip_file.writestr(file_name, data)
    with open('history_data.zip', 'rb') as f:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS history_data (data VARIANT)")
        cursor.execute("PUT file://" + 'history_data.zip' + " @%history_data")
        cursor.execute(
            "COPY INTO history_data from @%history_data FILE_FORMAT=(TYPE= 'csv')")
    cursor.close()
    ctx.close()
