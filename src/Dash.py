import time
import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(
    page_title="Activity Dashboard",
    page_icon=":bar_chart:",
    layout="wide"
)

df = pd.read_excel(
    io='../food.xlsx',
    engine='openpyxl',
    sheet_name='food',
    usecols='B:R',
    dtype='object'
)

# .... SIDEBAR ....
m = st.sidebar.multiselect(
    "Foods",
    options=df['email'].unique()
)

df_selection = df.query(
    "email == @m"
)

st.dataframe(df_selection)