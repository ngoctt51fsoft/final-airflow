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
from tool.DataTool import DataTool

with DAG(
    dag_id='baitap',
    start_date=datetime(2024, 1, 1),
    default_args={
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
},
    schedule_interval='@daily',
    catchup=False
) as dag:

    start = DummyOperator(task_id = "start")

    load_songs_task = PythonOperator(
        task_id='load_song_files_to_staging',
        python_callable=DataTool.load_song_files_into_staging,
        op_args=['my_postgres', 'data/song_data', 'staging_songs']
    )

    load_events_task = PythonOperator(
        task_id='load_event_files_to_staging',
        python_callable=DataTool.load_events_files_into_staging,
        op_args=['my_postgres', 'data/log_data', 'staging_events']
    )

    load_song_plays_fact_task = SQLExecuteQueryOperator(
        task_id='load_song_plays_fact',
        conn_id='my_postgres',
        sql = DataTool.load_sql('sql/load_song_plays_fact.sql')
    )

    load_song_dim_table = SQLExecuteQueryOperator(
        task_id='load_song_dim_table',
        conn_id='my_postgres',
        sql=DataTool.load_sql('sql/load_song_dim_table.sql')
    )

    load_user_dim_table = SQLExecuteQueryOperator(
        task_id='load_user_dim_table',
        conn_id='my_postgres',
        sql=DataTool.load_sql('sql/load_user_dim_table.sql')
    )
    load_artist_dim_table = SQLExecuteQueryOperator(
        task_id='load_artist_dim_table',
        conn_id='my_postgres',
        sql=DataTool.load_sql('sql/load_artist_dim_table.sql')
    )

    load_time_dim_table = SQLExecuteQueryOperator(
        task_id='load_time_dim_table',
        conn_id='my_postgres',
        sql=DataTool.load_sql('sql/load_time_dim_table.sql')
    )

    run_quality_checks_task = PythonOperator(
        task_id='run_quality_checks',
        python_callable=lambda: DataTool.run_quality_checks(['songs', 'users', 'artists', 'time', 'songplays'])
    )

    end = DummyOperator(task_id = "end")

    start >> [load_songs_task, load_events_task] >> load_song_plays_fact_task >> [load_song_dim_table, load_user_dim_table, load_artist_dim_table, load_time_dim_table]
    [load_song_dim_table, load_user_dim_table, load_artist_dim_table, load_time_dim_table] >> run_quality_checks_task >> end