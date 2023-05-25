import sqlite3
import pandas as pd
from glob import glob
from os.path import expanduser

# env path db, 
# path raw file name, 
# arg table, 


def pandas_sqlite_csv():
    conn = sqlite3.connect('/home/sergio/dev/python/music-analysis-pipeline/inputs/spotify.sqlite')
    # cursor = conn.cursor()
    # weirdos chars
    conn.text_factory = str
    table = pd.read_sql('SELECT * FROM albums LIMIT 100', conn)
    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/albums.csv', index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
