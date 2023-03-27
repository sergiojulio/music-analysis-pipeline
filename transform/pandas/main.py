import pandas as pd
import numpy as np

df = pd.read_csv('../../inputs/scrobbles-radioheadve-1679014882.csv')

df = df[df.artist == 'Radiohead']

df['new'] = df.groupby('artist')['artist'].transform('size')

print(df['new'])
