import sqlite3
import pandas as pd


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/spotify.sqlite')

    conn.text_factory = bytes

    for chunk in pd.read_sql('SELECT id, acousticness, analysis_url, danceability, duration, energy, instrumentalness, '
                             'key, liveness, loudness, mode, speechiness, tempo, time_signature, valence FROM '
                             'audio_features', conn, chunksize=100000):
        chunk['id'] = chunk['id'].str.decode('utf8', errors='ignore')

        chunk.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/audio_features.csv',
                     index=False, encoding='utf-8', mode='a')  # be carefoul with a mode, its adds row to current file


if __name__ == '__main__':
    pandas_sqlite_csv()
