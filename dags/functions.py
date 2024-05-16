import base64
import json
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from dotenv import load_dotenv
from requests import get, post

load_dotenv()

client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')


# Function to get the authentication token
def get_auth_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token


# Function to get the artist ID based on the artist name
def get_artist_id(artist, authorization_token):
    base_url = "https://api.spotify.com/v1/search"
    headers = authorization_token
    parameters = f"q={artist}&type=artist"
    url = base_url + "?" + parameters

    result = get(url, headers=headers)
    artists_info = result.json()

    if not artists_info['artists']['items']:
        return "No artists found."

    artist_info = artists_info['artists']['items'][0]
    artist_id = artist_info['id']

    return artist_id


# Function to get the album IDs for a given artist
def get_artist_album_ids(artist, artist_id, authorization_token):
    base_url = "https://api.spotify.com/v1/artists"
    headers = authorization_token
    parameters = f"/{artist_id}/albums?&limit=2"

    url = base_url + parameters
    result = get(url, headers=headers)

    if result.status_code == 200:
        albums_data = result.json()
    else:
        print(f"Request failed with status code: {result.status_code}")
        print(result.text)
        return []

    album_ids = []
    for album in albums_data['items']:
        if album['artists'][0]['name'] == artist:
            album_ids.append(album['id'])

    return album_ids


# Function to get the tracks for each album
def get_albums_tracks(**context):
    album_ids = context['task_instance'].xcom_pull(task_ids='get_artist_album_ids')
    access_token = context['task_instance'].xcom_pull(task_ids='get_auth_token')
    
    tracks_data = {
        'album_id': [],
        'track_id': [],
        'track_name': [],
        'track_number': [],
        'duration_ms': [],
        'explicit': [],
        'preview_url': []
    }

    for album_id in album_ids:
        url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=2"
        headers = {
            'Authorization': 'Bearer ' + access_token
        }

        response = get(url, headers=headers)
        album_tracks = response.json()

        if 'items' in album_tracks:
            for track in album_tracks['items']:
                tracks_data['album_id'].append(album_id)
                tracks_data['track_id'].append(track['id'])
                tracks_data['track_name'].append(track['name'])
                tracks_data['track_number'].append(track['track_number'])
                tracks_data['duration_ms'].append(track['duration_ms'])
                tracks_data['explicit'].append(track['explicit'])
                tracks_data['preview_url'].append(track['preview_url'])

    return tracks_data


# Function to insert the tracks data into the database
def insert_tracks_data(**context):
    tracks_data = context['task_instance'].xcom_pull(task_ids='get_album_tracks')
    
    postgres_hook = PostgresHook(postgres_conn_id='your_conn_id')
    
    insert_query = """
        INSERT INTO tswift.tracks (album_id, track_id, track_name, track_number, duration_ms, explicit, preview_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    for i in range(len(tracks_data['album_id'])):
        values = (
            tracks_data['album_id'][i],
            tracks_data['track_id'][i],
            tracks_data['track_name'][i],
            tracks_data['track_number'][i],
            tracks_data['duration_ms'][i],
            tracks_data['explicit'][i],
            tracks_data['preview_url'][i]
        )
        postgres_hook.run(insert_query, parameters=values)


# Function to fetch audio features for each track
def fetch_audio_features_from_dict(**context):
    tracks_data = context['task_instance'].xcom_pull(task_ids='get_album_tracks')
    authorization_token = context['task_instance'].xcom_pull(task_ids='get_auth_token')

    track_ids = tracks_data['track_id']

    id_list = []
    danceability_list = []
    energy_list = []
    key_list = []
    loudness_list = []
    mode_list = []
    speechiness_list = []
    acousticness_list = []
    instrumentalness_list = []
    liveness_list = []
    valence_list = []
    tempo_list = []
    duration_ms_list = []
    time_signature_list = []

    for track_id in track_ids:
        base_url = f"https://api.spotify.com/v1/audio-features/{track_id}"
        headers = {'Authorization': f'Bearer {authorization_token}'}

        result = get(base_url, headers=headers)
        audio_feat_track = result.json()

        id_list.append(audio_feat_track['id'])
        danceability_list.append(audio_feat_track['danceability'])
        energy_list.append(audio_feat_track['energy'])
        key_list.append(audio_feat_track['key'])
        loudness_list.append(audio_feat_track['loudness'])
        mode_list.append(audio_feat_track['mode'])
        speechiness_list.append(audio_feat_track['speechiness'])
        acousticness_list.append(audio_feat_track['acousticness'])
        instrumentalness_list.append(audio_feat_track['instrumentalness'])
        liveness_list.append(audio_feat_track['liveness'])
        valence_list.append(audio_feat_track['valence'])
        tempo_list.append(audio_feat_track['tempo'])
        duration_ms_list.append(audio_feat_track['duration_ms'])
        time_signature_list.append(audio_feat_track['time_signature'])

    audio_features_dict = {
        'id': id_list,
        'danceability': danceability_list,
        'energy': energy_list,
        'key': key_list,
        'loudness': loudness_list,
        'mode': mode_list,
        'speechiness': speechiness_list,
        'acousticness': acousticness_list,
        'instrumentalness': instrumentalness_list,
        'liveness': liveness_list,
        'valence': valence_list,
        'tempo': tempo_list,
        'duration_ms': duration_ms_list,
        'time_signature': time_signature_list
    }

    return audio_features_dict


# Function to insert audio features data into the database
def insert_audio_features_data(**context):
    audio_features_data = context['task_instance'].xcom_pull(task_ids='fetch_audio_features')
    
    postgres_hook = PostgresHook(postgres_conn_id='your_conn_id')
    
    insert_query = """
        INSERT INTO tswift.audio_features (
            id, danceability, energy, key, loudness, mode, speechiness, acousticness,
            instrumentalness, liveness, valence, tempo, duration_ms, time_signature
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for i in range(len(audio_features_data['id'])):
        values = (
            audio_features_data['id'][i],
            audio_features_data['danceability'][i],
            audio_features_data['energy'][i],
            audio_features_data['key'][i],
            audio_features_data['loudness'][i],
            audio_features_data['mode'][i],
            audio_features_data['speechiness'][i],
            audio_features_data['acousticness'][i],
            audio_features_data['instrumentalness'][i],
            audio_features_data['liveness'][i],
            audio_features_data['valence'][i],
            audio_features_data['tempo'][i],
            audio_features_data['duration_ms'][i],
            audio_features_data['time_signature'][i]
        )
        postgres_hook.run(insert_query, parameters=values)