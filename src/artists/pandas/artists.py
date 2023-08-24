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
    table = pd.read_sql('SELECT * FROM artists', conn)
    # """
    table['name'] = table['name'].str.decode('utf8', errors='ignore')
    table['id'] = table['id'].str.decode('utf8')
    table['popularity'] = table['popularity']
    table['followers'] = table['followers']
    # """
    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/artists.csv',
                 index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
