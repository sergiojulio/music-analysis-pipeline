import sqlite3
import pandas as pd


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/spotify.sqlite')

    conn.text_factory = bytes

    table = pd.read_sql('SELECT id, disc_number, duration, explicit, audio_feature_id, name, preview_url, '
                        'track_number, popularity, is_playable FROM audio_features', conn)

    table['name'] = table['name'].str.decode('utf8', errors='ignore')

    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/audio_features.csv',
                 index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
