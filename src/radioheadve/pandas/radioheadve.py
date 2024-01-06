import sqlite3
import pandas as pd

# env path db, 
# path raw file name, 
# arg table, 


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/radioheadve.sqlite')
    # Could not decode to UTF-8 column 'name'
    conn.text_factory = bytes

    table = pd.read_sql('SELECT uts, datetime, artist, album, track FROM tracks', conn)

    table['datetime'] = table['datetime'].str.decode('utf8')
    table['artist'] = table['artist'].str.decode('utf8', errors='ignore')
    table['album'] = table['album'].str.decode('utf8')
    table['track'] = table['track'].str.decode('utf8')
    table['id'] = table['uts'].str.decode('utf8')

    table = table.drop('uts', axis=1)

    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/radioheadve.csv',
                 index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
