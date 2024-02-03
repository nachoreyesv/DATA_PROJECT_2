import os
import shutil
import requests
import random
import pandas as pd
import xml.etree.ElementTree as ET

BASE_URL = 'http://127.0.0.1:5000'
NUM_OFERTAS = 3  # Número de ofertas que deseas generar
DOWNLOAD_FOLDER = 'get_coord'

def obtener_ruta_archivo_aleatorio():
    file_id = random.randint(1, 19)
    obtener_ruta_archivo(file_id)
    kml_file = os.path.join(DOWNLOAD_FOLDER, f'{file_id}.kml')
    return kml_file

def obtener_ruta_archivo(file_id):
    url = f'{BASE_URL}/get_kml/{file_id}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        file_path = data.get('file_path')
        if file_path:
            descargar_archivo(file_id, file_path)
        else:
            print(f'Error: No se ha podido obtener la ruta del archivo {file_id}')
    else:
        print(f'Error al obtener la ruta del archivo {file_id}. Código de estado: {response.status_code}')

def descargar_archivo(file_id, file_path):
    download_path = os.path.join(DOWNLOAD_FOLDER, f'{file_id}.kml')
    shutil.copy(file_path, download_path)

def read_kml(oferta, kml_file):
    data = {"id_oferta": [], "punto": [], "latitude": [], "longitude": []}

    with open(kml_file, "r", encoding="utf-8") as file:
        kml_data = file.read()

    root = ET.fromstring(kml_data)
    coords = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")

    if coords is not None:
        coords_str = coords.text
        coords_list = [tuple(map(float, _.split(','))) for _ in coords_str.split()]

        for _, coords in enumerate(coords_list):
            data["id_oferta"] = oferta
            data["punto"] = _ + 1
            data["latitude"] = coords[1]
            data["longitude"] = coords[0]
            print(data)

def gen_ofertas(num_ofertas):
    for i in range(1, num_ofertas + 1):
        kml_file = obtener_ruta_archivo_aleatorio()
        read_kml(oferta=i, kml_file=kml_file)

if __name__ == '__main__':
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    gen_ofertas(NUM_OFERTAS)
