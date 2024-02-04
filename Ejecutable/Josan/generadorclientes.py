# Solamente he pegado una copia del de vehiculos

import os
import shutil
import requests
import random
import pandas as pd
import xml.etree.ElementTree as ET

from google.cloud import pubsub_v1
import argparse
import logging
import string
import json
import time

BASE_URL = 'http://127.0.0.1:5000'
NUM_OFERTAS = 3  # Número de ofertas que deseas generar
DOWNLOAD_FOLDER = 'get_coord'


#Input arguments
parser = argparse.ArgumentParser(description=('Streaming Data Generator'))

parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()

class PubSubMessages:

    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publish_message(self, data):
        json_str = json.dumps(data)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")



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

def read_kml(oferta, kml_file, project_id: str, topic_name: str):

    pubsub_class = PubSubMessages(project_id, topic_name)

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
            pubsub_class.publish_message(data)
            time.sleep(5)



def gen_ofertas(num_ofertas, project_id, topic_name):

    for i in range(1, num_ofertas + 1):
        kml_file = obtener_ruta_archivo_aleatorio()
        read_kml(oferta=i, kml_file=kml_file, project_id=project_id, topic_name=topic_name)



if __name__ == '__main__':
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    gen_ofertas(NUM_OFERTAS, args.project_id, args.topic_name)
