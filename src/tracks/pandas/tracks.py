import sqlite3
import pandas as pd

# env path db, 
# path raw file name, 
# arg table, 


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/spotify.sqlite')
    # Could not decode to UTF-8 column 'name'
    conn.text_factory = bytes
    # cursor = conn.cursor()
    # weirdos chars
    # conn.text_factory = str
    table = pd.read_sql('SELECT id, disc_number, duration, explicit, audio_feature_id, name, preview_url, '
                        'track_number, popularity, is_playable FROM tracks', conn)

    table['name'] = table['name'].str.decode('utf8', errors='ignore')

    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/tracks.csv',
                 index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
