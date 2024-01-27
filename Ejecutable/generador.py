import pandas as pd
import os
import xml.etree.ElementTree as ET
import streamlit as st
from streamlit_folium import folium_static

# LOCAL IMPORTS

from functions import kml_df, gen_ofertas, gen_solicitudes, calcular_coste, mapa

if __name__ == '__main__':

    df = kml_df('coordenadas')

    dfs_list = gen_ofertas(df)

    solicitudes = gen_solicitudes(dfs_list)

    coste = calcular_coste(solicitudes)

    print(dfs_list)
    print(solicitudes)
    print(coste)

    folium_static(mapa(dfs_list, solicitudes))