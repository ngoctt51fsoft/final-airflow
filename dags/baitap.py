from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
from datetime import timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def load_song_files_into_postgres():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres')
    for root, dirs, files in os.walk("data/song_data"):
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
                        pg_hook.insert_rows(table="staging_songs", rows=[row])

def load_events_into_postgres():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres')
    rows_to_insert = []
    def safe_int(val):
        if val in (None, ""):
            return None
        try:
            return int(val)
        except Exception:
            return None

    for root, dirs, files in os.walk("data/log_data"):
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
                            safe_int(item.get('userId'))
                        )
                        rows_to_insert.append(row)
    if rows_to_insert:
        pg_hook.insert_rows(table='staging_events', rows=rows_to_insert)


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='baitap',
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    start = DummyOperator(task_id = "start")

    load_songs_task = PythonOperator(
        task_id='load_song_files_to_postgres',
        python_callable=load_song_files_into_postgres
    )

    load_events_task = PythonOperator(
        task_id='load_events_to_postgres',
        python_callable=load_events_into_postgres
    )

    load_song_plays_fact_task = SQLExecuteQueryOperator(
        task_id='load_song_plays_fact',
        conn_id='my_postgres',
        sql = """
    insert into public.songplays select
            md5(cast(events.sessionid as TEXT) || cast(events.start_time as TEXT)) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
""")

    load_song_dim_table = SQLExecuteQueryOperator(
        task_id='load_song_dim_table',
        conn_id='my_postgres',
        sql="""
    insert into songs select distinct song_id, title, artist_id, year, duration
    FROM staging_songs
"""
    )

    load_user_dim_table = SQLExecuteQueryOperator(
        task_id='load_user_dim_table',
        conn_id='my_postgres',
        sql="""
    insert into users select distinct userid, firstname, lastname, gender
    FROM staging_events
    WHERE page='NextSong' and userid IS NOT NULL
"""
    )
    load_artist_dim_table = SQLExecuteQueryOperator(
        task_id='load_artist_dim_table',
        conn_id='my_postgres',
        sql="""
    insert into artists SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
"""
    )

    load_time_dim_table = SQLExecuteQueryOperator(
        task_id='load_time_dim_table',
        conn_id='my_postgres',
        sql="""
    insert into time SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
            extract(month from start_time), extract(year from start_time), extract(dow from start_time)
    FROM songplays
"""
    )

    def run_quality_checks(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres')
        tables = ['songs', 'users', 'artists', 'time']
        for table_name in tables:
            records = pg_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or records[0][0] < 1:
                raise ValueError(f"Data quality check failed for table {table_name}: 0 rows found.")
            logging.info(f"Data quality check passed for table {table_name}: {records[0][0]} rows found.")

    run_quality_checks_task = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks
    )

    end = DummyOperator(task_id = "end")

    start >> load_songs_task >> load_events_task >> load_song_plays_fact_task >> [load_song_dim_table, load_user_dim_table, load_artist_dim_table, load_time_dim_table]
    [load_song_dim_table, load_user_dim_table, load_artist_dim_table, load_time_dim_table] >> run_quality_checks_task >> end