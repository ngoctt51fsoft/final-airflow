import os
import json
import logging
from airflow.hooks.postgres_hook import PostgresHook

class DataTool:
    @staticmethod
    def load_song_files_into_staging(postgres_conn_id, data_dir, table_name):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        rows_to_insert = []
        for root, dirs, files in os.walk(data_dir):
            for filename in files:
                if filename.endswith(".json"):
                    file_path = os.path.join(root, filename)
                    with open(file_path, 'r') as f:
                        data = [json.loads(line) for line in f]
                        for item in data:
                            row = (
                                item.get('num_songs'),
                                item.get('artist_id'),
                                item.get('artist_name'),
                                item.get('artist_latitude'),
                                item.get('artist_longitude'),
                                item.get('artist_location'),
                                item.get('song_id'),
                                item.get('title'),
                                item.get('duration'),
                                item.get('year')
                            )
                            rows_to_insert.append(row)
        if rows_to_insert:
            pg_hook.insert_rows(table=table_name, rows=rows_to_insert)

    @staticmethod
    def str_to_int(val):
        if val in (None, ""):
            return None
        try:
            return int(val)
        except Exception:
            return None

    @staticmethod
    def load_events_files_into_staging(postgres_conn_id, data_dir, table_name):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        rows_to_insert = []
        for root, dirs, files in os.walk(data_dir):
            for filename in files:
                if filename.endswith(".json"):
                    file_path = os.path.join(root, filename)
                    with open(file_path, 'r') as f:
                        for line in f:
                            item = json.loads(line)
                            row = (
                                item.get('artist'),
                                item.get('auth'),
                                item.get('firstName'),
                                item.get('gender'),
                                item.get('itemInSession'),
                                item.get('lastName'),
                                item.get('length'),
                                item.get('level'),
                                item.get('location'),
                                item.get('method'),
                                item.get('page'),
                                item.get('registration'),
                                item.get('sessionId'),
                                item.get('song'),
                                item.get('status'),
                                item.get('ts'),
                                item.get('userAgent'),
                                DataTool.str_to_int(item.get('userId'))
                            )
                            rows_to_insert.append(row)
        if rows_to_insert:
            pg_hook.insert_rows(table=table_name, rows=rows_to_insert)

    @staticmethod
    def run_quality_checks(tables, postgres_conn_id='my_postgres'):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        for table_name in tables:
            records = pg_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or records[0][0] < 1:
                raise ValueError(f"Data quality check failed for table {table_name}: 0 rows found.")
            logging.info(f"Data quality check passed for table {table_name}: {records[0][0]} rows found.")

    @staticmethod
    def load_sql(filename):
        with open(filename) as f:
            return f.read()
