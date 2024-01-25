import pandas as pd
import os
import xml.etree.ElementTree as ET
import random


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
    
    for i in range(5):
        a = random.randint(1,11)
        res.append(a)

    for i in res:
        matching_rows = df[df['viaje'] == i]
        if not matching_rows.empty:
            inicio_aleatorio = random.randint(0, len(matching_rows) - 100)
            valores_seleccionados = matching_rows[inicio_aleatorio:inicio_aleatorio + 100]
            ofertas.append(valores_seleccionados)

    return ofertas

def gen_solicitudes(dfs_list):
    solicitudes = []

    for i in dfs_list:
        puntos_posibles = i['punto_total'].to_list()
        punto_inicio = random.choice(puntos_posibles)
        punto_final = random.randint(punto_inicio + 10, i['punto_total'].max() + 1)  # cambiable, valorar si hace trayecto completo o baja antes 
        solicitudes.append((punto_inicio, punto_final))

    return solicitudes

def calcular_coste(solicitudes):
    tarifa_por_punto = 0.50  # cambiable, tarifa por cada punto avanzado
    costes = []

    for trayecto in solicitudes:
        punto_inicio, punto_final = trayecto
        recorrido = punto_final - punto_inicio
        coste_trayecto = recorrido * tarifa_por_punto
        costes.append(coste_trayecto)
        
    return costes