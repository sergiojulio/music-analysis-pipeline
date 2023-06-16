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
    table = pd.read_sql('SELECT * FROM albums', conn)
    #
    table['id'] = table['id'].str.decode('latin-1').str.decode('utf8')
    # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xed in position 16: invalid continuation byte
    table['name'] = table['name'].str.decode('latin-1').str.decode('utf8')
    table['album_group'] = table['album_group'].str.decode('latin-1').str.decode('utf8')
    table['album_type'] = table['album_type'].str.decode('latin-1').str.decode('utf8')
    #
    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/albums.csv',
                 index=False, encoding='utf8')


if __name__ == '__main__':
    pandas_sqlite_csv()
