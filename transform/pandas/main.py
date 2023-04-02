import pandas as pd
import numpy as np
import great_expectations as ge

df = pd.read_csv('../../inputs/scrobbles-radioheadve-1679014882.csv', index_col='uts')

df_ge = ge.from_pandas(df)

print(df_ge.head())
#print(df_ge)

print(df_ge.expect_column_values_to_be_unique(column="utc_time"))


# print(df['artist'].value_counts().head(10))
# df.groupBy('artist').count().orderBy('count', ascending=False).show()
# print(df.iloc[1])
