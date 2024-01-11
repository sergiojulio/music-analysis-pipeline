import streamlit as st
import duckdb

con = duckdb.connect(database='/home/sergio/dev/python/music-analysis-pipeline/datalake/db.duckdb', read_only=True)

st.set_page_config(layout="wide")

st.title('Dickdb + Streamlit')

df = con.execute("""
    SELECT * 
    FROM top_artists_model 
    LIMIT 10
    """).df()

st.table(df)
