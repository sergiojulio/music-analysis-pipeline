import sqlite3
import pandas as pd


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/spotify.sqlite')

    # conn.text_factory = bytes

    # delete cvs file if exists

    for chunk in pd.read_sql('SELECT track_id, artist_id FROM r_track_artist', conn, chunksize=100000):
        # chunk['id'] = chunk['id'].str.decode('utf8', errors='ignore')

        chunk.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/r_track_artist.csv',
                     index=False, encoding='utf-8', mode='a')  # be carefoul with a mode, its adds row to current file


if __name__ == '__main__':
    pandas_sqlite_csv()
