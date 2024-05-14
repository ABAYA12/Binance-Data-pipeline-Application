# postgres_integration.py
import psycopg2
import zipfile


def persist_to_postgres(data, host, database, user, password, port, file_name):
    conn = psycopg2.connect(host=host, database=database,
                            user=user, password=password, port=port)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS history_data (data BYTEA)")
    with open('history_data.zip', 'rb') as f:
        cursor.execute("INSERT INTO history_data (data) VALUES (%s)",
                       (psycopg2.Binary(f.read()),))
    conn.commit()
    cursor.close()
    conn.close()
