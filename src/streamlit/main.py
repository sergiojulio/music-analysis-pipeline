import streamlit as st
import duckdb

con = duckdb.connect(database='/home/sergio/dev/python/music-analysis-pipeline/datalake/db.duckdb', read_only=True)

st.set_page_config(layout="wide")

st.title('Dickdb + Streamlit')

