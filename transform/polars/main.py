import polars as pl

df = pl.read_csv('../../inputs/scrobbles-radioheadve-1679014882.csv', parse_dates=False)

print(df.head(5))
