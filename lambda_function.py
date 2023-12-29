import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import requests
import json
import base64
import pandas as pd
import numpy as np
import s3fs

client_id = '***'
client_secret = '***'
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def get_headers(client_id, client_secret):
    endpoint = 'https://accounts.spotify.com/api/token'
    encoded = base64.b64encode((client_id+':'+client_secret).encode('utf-8')).decode('ascii')
    headers = {'Authorization': 'Basic {auth}'.format(auth=encoded)}
    payload = {'grant_type':'client_credentials'}
    r = requests.post(endpoint, data=payload, headers=headers)
    acess_token = json.loads(r.text).get('access_token')
    headers = {'Authorization': 'Bearer {auth}'.format(auth=acess_token)}
    return headers
    
def get_album(album_id):
    endpoint = f'https://api.spotify.com/v1/albums/{album_id}'
    headers = get_headers(client_id, client_secret)
    r = requests.get(endpoint, headers=headers)
    return json.loads(r.text)

def get_artist(artist_id):
    endpoint = f'https://api.spotify.com/v1/artists/{artist_id}'
    headers = get_headers(client_id, client_secret)
    r = requests.get(endpoint, headers=headers)
    return json.loads(r.text)

def get_genres(album_id):
    album = get_album(album_id)
    genres = album.get('genres')
    artists = album['artists']
    for artist in artists:
        artist_id = artist['id']
        artist_info = get_artist(artist_id)
        genres = artist_info['genres']
    return genres
    
new_release = {}

def new_release_album(client_id, client_secret):
    endpoint = 'https://api.spotify.com/v1/browse/new-releases'
    headers = get_headers(client_id, client_secret)
    params = {
        'limit':50
    }
    r = requests.get(endpoint, params = params, headers = headers)

    if r.status_code == 200:
        data = json.loads(r.text)
        for d in data.get('albums').get('items'):
            for a in d.get('artists'):
                artist = a['name']
                artist_id = a['id']
            album_name = d.get('name')
            album_id = d.get('id')
            date = d.get('release_date')
            genres = get_genres(album_id)
            genres = ','.join(genres)
            if album_id not in new_release.keys():
                new_release[album_id] = {'album':album_name, 'album_id': album_id, 'date':date, 'artist': artist, 'artist_id':artist_id, 'genres': genres}
                latest_date = date
    else:
        print('error! status code: ', r.status_code)
    return new_release


def lambda_handler(event, context):
    new_release_album(client_id, client_secret)
    spo_df = pd.DataFrame(new_release)
    spo_df = spo_df.transpose()
    spo_df.to_parquet(s3Url + s3Path + filename,
                      index=False,
                      storage_options={'key':'***',
                                       'secret':'***'}
    )
    resultFileName = s3_put_json(f's3://{bucketName}/', s3Path, filename)
    return 'Parquet file created!'
    
    

"""
테스트 입력 데이터

{
    "bucketName" : "spotify-etl-bk",
    "s3Path" : "parquet/",
    "filename" : "spotify-2.parquet"
}
"""

"""
Runtime.ImportModuleError: Unable to import module 'lambda_function': Unable to import required dependencies:
numpy: Error importing numpy: you should not try to import numpy from
        its source directory; please exit the numpy source tree, and relaunch
        your python interpreter from there.

numpy import 오류로 아직 테스트 해보지 못함
"""