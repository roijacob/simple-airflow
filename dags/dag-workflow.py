import os
from dotenv import load_dotenv
from requests import post, get

from datetime import datetime, timedelta
import json
import base64
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from functions import *  # Corrected module name

#%% Configuration
artist_name = 'Fiji Blue'

#%% DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now().strftime('%Y-%m-%d'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

#%% Create the DAG
dag = DAG(
    'spotify_artist_info',  # Updated DAG name
    default_args=default_args,
    description='DAG to search for artist information on Spotify',
    schedule=timedelta(days=1),
)

#%% Task: Get authentication token
get_auth_token_task = PythonOperator(
    task_id='get_auth_token',
    python_callable=get_auth_token,
    dag=dag,
)

#%% Task: Get artist ID
get_artist_id_task = PythonOperator(
    task_id='get_artist_id',
    python_callable=get_artist_id,
    op_kwargs={
        'artist': artist_name,
        'authorization_token': {
            'Authorization': 'Bearer ' + "{{ task_instance.xcom_pull(task_ids='get_auth_token') }}"
        }
    },
    dag=dag,
)

#%% Task: Get artist album IDs
get_artist_album_ids_task = PythonOperator(
    task_id='get_artist_album_ids',
    python_callable=get_artist_album_ids,
    op_kwargs={
        'artist': artist_name,
        'artist_id': "{{ task_instance.xcom_pull(task_ids='get_artist_id') }}",
        'authorization_token': {
            'Authorization': 'Bearer ' + "{{ task_instance.xcom_pull(task_ids='get_auth_token') }}"
        }
    },
    dag=dag,
)

#%% Task: Get album tracks
get_album_tracks_task = PythonOperator(
    task_id='get_album_tracks',
    python_callable=get_albums_tracks,
    provide_context=True,
    dag=dag,
)

create_tracks_table_task = PostgresOperator(
    task_id='create_tracks_table',
    postgres_conn_id='your_conn_id',
    sql='''
        CREATE TABLE IF NOT EXISTS spotify.tracks (
            album_id TEXT,
            track_id TEXT,
            track_name TEXT,
            track_number INTEGER,
            duration_ms INTEGER,
            explicit BOOLEAN,
            preview_url TEXT
        )
    ''',
    dag=dag,
)

insert_tracks_task = PythonOperator(
    task_id='insert_tracks_data',
    python_callable=insert_tracks_data,
    provide_context=True,
    dag=dag,
)

fetch_audio_features_task = PythonOperator(
    task_id='fetch_audio_features',
    python_callable=fetch_audio_features_from_dict,
    provide_context=True,
    dag=dag,
)

create_audio_features_table_task = PostgresOperator(
    task_id='create_audio_features_table',
    postgres_conn_id='your_conn_id',
    sql='''
        CREATE TABLE IF NOT EXISTS spotify.audio_features (
            id TEXT,
            danceability FLOAT,
            energy FLOAT,
            key INTEGER,
            loudness FLOAT,
            mode INTEGER,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            duration_ms INTEGER,
            time_signature INTEGER
        )
    ''',
    dag=dag,
)

insert_audio_features_task = PythonOperator(
    task_id='insert_audio_features_data',
    python_callable=insert_audio_features_data,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
get_auth_token_task >> get_artist_id_task >> get_artist_album_ids_task >> get_album_tracks_task
get_album_tracks_task >> create_tracks_table_task >> insert_tracks_task
get_album_tracks_task >> fetch_audio_features_task >> create_audio_features_table_task >> insert_audio_features_task