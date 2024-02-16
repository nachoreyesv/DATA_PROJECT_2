import pandas as pd
from pandas_gbq import read_gbq
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import folium_static

project_id = 'dataflow-clase'

query_vehiculos = 'SELECT * FROM `data_project_2.tabla_vehiculos`'
vehiculos_df = read_gbq(query_vehiculos, project_id=project_id, dialect='standard')
vehiculos_df = vehiculos_df.sort_values(by=['id_vehiculo', 'punto'])

query_usuarios = 'SELECT * FROM `data_project_2.tabla_usuarios`'
usuarios_df = read_gbq(query_usuarios, project_id=project_id, dialect='standard')
usuarios_df = usuarios_df.sort_values(by='id_usuario')

def mapa_con_usuarios(vehiculos_df, usuarios_df):

    folium_map = folium.Map(location=[vehiculos_df['latitud'].mean(), vehiculos_df['longitud'].mean()], tiles="cartodb positron", zoom_start=14)
    ids_vehiculo = vehiculos_df['id_vehiculo'].unique()

    color_vehiculos = 'red'
    color_usuarios = 'green'

    for i, id_vehiculo in enumerate(ids_vehiculo):
        df_vehiculo = vehiculos_df[vehiculos_df['id_vehiculo'] == id_vehiculo]
        locations = df_vehiculo[['latitud', 'longitud']].values.tolist()

        folium.PolyLine(
            locations=locations,
            color=color_vehiculos,
            weight=2.5,
            opacity=1
        ).add_to(folium_map)

        # Añadir marcador al primer punto de la ruta del vehículo
        vehiculo_inicio = df_vehiculo.iloc[0]
        folium.Marker(
            location=[vehiculo_inicio['latitud'], vehiculo_inicio['longitud']],
            popup=f'Inicio Viaje Nº {vehiculo_inicio["id_viaje"]}',
            icon=folium.Icon(color='magenta', icon="fa-car", prefix='fa')
        ).add_to(folium_map)

    for _, row in usuarios_df.iterrows():
        folium.CircleMarker(
            location=[row['latitud'], row['longitud']],
            popup=f'Usuario Nº {row["id_usuario"]}',
            radius=3,
            color=color_usuarios,
            fill=True,
            fill_color=color_usuarios
        ).add_to(folium_map)

    return folium_map

folium_static(mapa_con_usuarios(vehiculos_df, usuarios_df))
