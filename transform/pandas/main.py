import pandas as pd
import numpy as np

df = pd.read_csv('../../inputs/scrobbles-radioheadve-1679014882.csv', index_col='uts')

# print(df['artist'].value_counts().head(10))
# df.groupBy('artist').count().orderBy('count', ascending=False).show()

print(df.iloc[1])
