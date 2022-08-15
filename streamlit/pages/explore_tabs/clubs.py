import streamlit as st
from utils import load_asset, st_td_asset_summary

def clubs_tab():

    asset_name = "base_clubs"

    st_td_asset_summary(asset_name)
