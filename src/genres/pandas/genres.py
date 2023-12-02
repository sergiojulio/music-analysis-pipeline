import sqlite3
import pandas as pd

# env path db, 
# path raw file name, 
# arg table, 


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/spotify.sqlite')

    conn.text_factory = bytes

    table = pd.read_sql('SELECT id FROM genres', conn)

    table['id'] = table['id'].str.decode('utf8', errors='ignore')

    # """
    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/genres.csv',
                 index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
