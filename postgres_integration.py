"""
Module for persisting data to PostgreSQL.
"""


import zipfile
import psycopg2


def persist_to_postgres(data, host, database, user, password, port):
    """
    Persists data to PostgreSQL.

    Args:
        data: Data to be persisted.
        host (str): Host address of the PostgreSQL server.
        database (str): Name of the database.
        user (str): Username for authentication.
        password (str): Password for authentication.
        port (int): Port number of the PostgreSQL server.
    """
    conn = psycopg2.connect(host=host, database=database,
                            user=user, password=password, port=port)
    cursor = conn.cursor()
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

    # Check if the zip file exists
    try:
        with open('./history_data.zip', 'rb') as f:
            cursor.execute("INSERT INTO history_data (data) VALUES (%s)",
                           (psycopg2.Binary(f.read()),))
        conn.commit()
        print("Data inserted into Postgres successfully.")
    except FileNotFoundError:
        print("Error: history_data.zip not found. Make sure the file exists.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    pass  # Add main code logic here if needed


# import psycopg2
# import zipfile


# def persist_to_postgres(data, host, database, user, password, port, file_name):
#     conn = psycopg2.connect(host=host, database=database,
#                             user=user, password=password, port=port)
#     cursor = conn.cursor()
#     cursor.execute("CREATE TABLE IF NOT EXISTS history_data (data BYTEA)")

#     # Check if the zip file exists
#     try:
#         with open('./history_data.zip', 'rb') as f:
#             cursor.execute("INSERT INTO history_data (data) VALUES (%s)",
#                            (psycopg2.Binary(f.read()),))
#         conn.commit()
#         print("Data inserted into Postgres successfully.")
#     except FileNotFoundError:
#         print("Error: history_data.zip not found. Make sure the file exists.")

#     cursor.close()
#     conn.close()
