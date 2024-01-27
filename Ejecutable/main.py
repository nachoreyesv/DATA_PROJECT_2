import pandas as pd
import os
import xml.etree.ElementTree as ET
import streamlit as st
import random
import folium
from streamlit_folium import folium_static

def kml_df(kml_folder_path):
    data = {"viaje": [], "punto": [], "latitude": [], "longitude": [], "altitude": []}
    viaje_count = 0

    for _ in os.listdir(kml_folder_path):
        if _.endswith(".kml"):
            kml_file_path = os.path.join(kml_folder_path, _)

            with open(kml_file_path, "r", encoding="utf-8") as file:
                kml_data = file.read()

            root = ET.fromstring(kml_data)
            coordinates_element = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")

            if coordinates_element is not None:
                coordinates_str = coordinates_element.text
                coordinates_list = [tuple(map(float, _.split(','))) for _ in coordinates_str.split()]

                viaje_count += 1
                viaje_key = viaje_count

                for _, coords in enumerate(coordinates_list):
                    data["viaje"].append(viaje_key)
                    data["punto"].append(_ + 1)
                    data["latitude"].append(coords[1])
                    data["longitude"].append(coords[0])
                    data["altitude"].append(coords[2])

    df = pd.DataFrame(data)
    df['punto_total'] = df.index
    return df

def gen_ofertas(df):
    res = []
    ofertas = []
    no_rep = list(range(1,11))

    for i in range(5):
        a = random.choice(no_rep)
        res.append(a)
        no_rep.remove(a)
    
    for i in res:
        matching_rows = df[df['viaje'] == i]

        if not matching_rows.empty:
            ofertas.append(matching_rows)

    return ofertas

def gen_solicitudes(dfs_list, distancia_minima=40):
    solicitudes = []

    for i in dfs_list:
        puntos_posibles = i['punto_total'].to_list()
        punto_inicio = random.choice(puntos_posibles[:len(puntos_posibles) // 2])
        indices_validos = [idx for idx, punto in enumerate(puntos_posibles[len(puntos_posibles) // 2:]) if punto - punto_inicio >= distancia_minima]

        if indices_validos:
            indice_final = random.choice(indices_validos)
            punto_final = puntos_posibles[len(puntos_posibles) // 2:][indice_final]
            solicitudes.append((punto_inicio, punto_final))

    return solicitudes

def calcular_coste(solicitudes):
    tarifa_por_punto = 0.50 
    costes = []

    for trayecto in solicitudes:
        punto_inicio, punto_final = trayecto
        recorrido = punto_final - punto_inicio
        coste_trayecto = recorrido * tarifa_por_punto
        costes.append(coste_trayecto)
        
    return costes

def mapa(dfs_list, solicitudes):
    
    folium_map = folium.Map(location=[dfs_list[0]['latitude'].mean(), dfs_list[0]['longitude'].mean()], tiles="cartodb positron", zoom_start=14)

    colors = ['blue', 'red', 'green', 'yellow', 'black']

    for i, df in enumerate(dfs_list):

        folium.Marker(
        location=(df['latitude'].iloc[0], df['longitude'].iloc[0]),
        popup='Inicio',
        icon=folium.Icon(color='purple', icon='star')
        ).add_to(folium_map)

        # Marcador ruta conductor
        folium.PolyLine(
            locations=df[['latitude', 'longitude']].values,
            color=colors[i],
            weight=2.5,
            opacity=1
        ).add_to(folium_map)

        #folium.Marker(
        #popup='Fin',
        #location=(df['latitude'].iloc[-1], df['longitude'].iloc[-1]),
        #icon=folium.Icon(color='orange', icon='flag')
        #).add_to(folium_map)


    for solicitud in solicitudes:
        punto_inicio, punto_final = solicitud

        df_puntoinicio = None
        df_puntofinal = None
        i = 0

        for df in dfs_list:
            i += 1
            if punto_inicio in df['punto_total'].values:
                df_puntoinicio = df[df['punto_total'] == punto_inicio]
                break

        for df in dfs_list:
            if punto_final in df['punto_total'].values:
                df_puntofinal = df[df['punto_total'] == punto_final]
                break

        if not df_puntoinicio.empty:
            latitude_inicio = df_puntoinicio['latitude'].values[0]
            longitude_inicio = df_puntoinicio['longitude'].values[0]

            # Marcador solicitud inicio (lugar donde se sube el cliente)
            folium.Marker(
                location=[latitude_inicio, longitude_inicio],
                popup=f'Cliente sube al coche en el punto {punto_inicio} de la oferta:{i}',
                icon=folium.Icon(color='green', icon='user')
            ).add_to(folium_map)

        if df_puntofinal is not None and not df_puntofinal.empty:
            latitude_final = df_puntofinal['latitude'].values[0]
            longitude_final = df_puntofinal['longitude'].values[0]

            # Marcador solicitud final (lugar donde se baja el cliente)
            folium.Marker(
                location=[latitude_final, longitude_final],
                popup=f'Cliente baja del coche en el punto {punto_final} de la oferta:{i}',
                icon=folium.Icon(color='red', icon='user')
            ).add_to(folium_map)


    return folium_map


if __name__ == '__main__':

    df = kml_df('coordenadas')

    dfs_list = gen_ofertas(df)

    solicitudes = gen_solicitudes(dfs_list)

    coste = calcular_coste(solicitudes)

    print(dfs_list)
    print(solicitudes)
    print(coste)

    folium_static(mapa(dfs_list, solicitudes))
