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
    table = pd.read_sql('SELECT id FROM genres', conn)
    # ""
    table['id'] = table['id'].str.decode('utf8')
    # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xed in position 16: invalid continuation byte
    #
    table['name'] = table['name'].str.decode('utf8', errors='ignore')
    table['album_group'] = table['album_group'].str.decode('utf8')
    table['album_type'] = table['album_type'].str.decode('utf8')
    # Dq..
    table.loc[table['release_date'] == 0, 'release_date'] = -2208902400000
    table.loc[table['release_date'] < -2208902400000, 'release_date'] = -2208988800000
    # aqui hay que generar un hash que sea el id
    # luego en tabla link artist_genre hay que reemplazar el name por el id
    # hay que crear un new file
    table['popularity'] = table['popularity']
    # """
    table.to_csv('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/tracks.csv',
                 index=False, encoding='utf-8')


if __name__ == '__main__':
    pandas_sqlite_csv()
