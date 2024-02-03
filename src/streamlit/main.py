import streamlit as st
import duckdb

con = duckdb.connect(database='/home/sergio/dev/python/music-analysis-pipeline/datalake/db.duckdb', read_only=True)

st.set_page_config(layout="wide")
st.header("Filter and edit data")
st.title('Duckdb + Streamlit')

df = con.execute("""
    SELECT * 
    FROM top_artists_model 
    LIMIT 10
    """).df()

st.table(df)

st.button("Reset", type="primary")
if st.button('Say hello'):
    st.write('Why hello there')
else:
    st.write('Goodbye')
