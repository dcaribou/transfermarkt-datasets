import streamlit as st

from utils import st_td_asset_summary

def competitions_tab():
    st_td_asset_summary("cur_competitions")
