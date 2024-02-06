import os
import random
import json
import time
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud.storage import Blob
import xml.etree.ElementTree as ET
import argparse
import logging

BASE_URL = 'http://127.0.0.1:5000'
NUM_OFERTAS = 1
NUM_SOLICITUDES = 10
DOWNLOAD_FOLDER = 'get_coord'

parser = argparse.ArgumentParser(description=('Streaming Data Generator'))

parser.add_argument(
    '--project_id',
    required=True,
    help='GCP cloud project name.')
parser.add_argument(
    '--topic_ofertas',
    required=True,
    help='PubSub topic de ofertas.')
parser.add_argument(
    '--topic_solicitudes',
    required=True,
    help='PubSub topic de solicitudes.')
parser.add_argument(
    '--bucket_name',
    required=True,
    help='Google Cloud Storage bucket name.')

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

    def close(self):
        self.publisher.api.transport.close()
        logging.info(f"PubSub Client for {self.topic_name} closed.")



def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = Blob(source_blob_name, bucket)
    blob.download_to_filename(destination_file_name)


def read_kml(oferta, bucket_name, file_id, project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)

    kml_file = os.path.join(DOWNLOAD_FOLDER, f'{file_id}.kml')
    download_blob(bucket_name, f'{file_id}.kml', kml_file)

    data = {"id_oferta": [], "punto": [], "latitude": [], "longitude": [], "trayecto": []}
    datos_longitude = []
    datos_latitude = []

    with open(kml_file, "r", encoding="utf-8") as file:
        kml_data = file.read()

    root = ET.fromstring(kml_data)
    coords = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")

    if coords is not None:
        coords_str = coords.text
        coords_list = [tuple(map(float, _.split(',')))[:2] for _ in coords_str.split()]

        for _, coords in enumerate(coords_list):

            data["id_oferta"] = oferta
            data["punto"] = _ + 1
            data["latitude"] = coords[1]
            data["longitude"] = coords[0]
            data["trayecto"] = coords_list[_:]
            datos_latitude.append(coords[1])
            datos_longitude.append(coords[0])
            print(data)
            pubsub_class.publish_message(data)
            time.sleep(1)


    return datos_longitude, datos_latitude


def get_coords_finales(bucket_name, DOWNLOAD_FOLDER):
    latitudes_finales = []
    longitudes_finales = []

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        if blob.name.endswith(".kml"):
            kml_file = os.path.join(DOWNLOAD_FOLDER, blob.name)

            with open(kml_file, "wb") as file:
                blob.download_to_file(file)

            with open(kml_file, "r", encoding="utf-8") as archivo:
                datos_kml = archivo.read()

            raiz = ET.fromstring(datos_kml)
            coordenadas = raiz.findall(".//{http://www.opengis.net/kml/2.2}coordinates")

            if coordenadas:
                for coordenada_str in coordenadas:
                    coords_lista = [tuple(map(float, _.split(','))) for _ in coordenada_str.text.split()]
                    for coords in coords_lista:
                        longitudes_finales.append(coords[0])
                        latitudes_finales.append(coords[1])

    return latitudes_finales, longitudes_finales


def gen_ofertas(num_ofertas, project_id, topic_name, bucket_name):
    datos_longitude_total = []
    datos_latitude_total = []

    for i in range(1, num_ofertas + 1):
        file_id = random.randint(1, 27)
        datos_longitude, datos_latitude = read_kml(
            oferta=i, bucket_name=bucket_name, file_id=file_id, project_id=project_id, topic_name=topic_name)
        datos_longitude_total = datos_longitude
        datos_latitude_total = datos_latitude

    return datos_longitude_total, datos_latitude_total


def gen_solicitudes(num_solicitudes, project_id, topic_name, datos_latitude_total, datos_longitude_total, latitudes_finales, longitudes_finales):

    pubsub_class = PubSubMessages(project_id, topic_name)

    data_solicitud = {"id_solicitud": [], "latitude": [], "longitude": [], "latitude_destino": [], "longitude_destino": []}
    for i in range(1, num_solicitudes + 1):
        data_solicitud['id_solicitud'] = i
        data_solicitud['latitude'] = random.choice(datos_latitude_total)
        data_solicitud['longitude'] = random.choice(datos_longitude_total)
        data_solicitud['latitude_destino'] = random.choice(latitudes_finales)
        data_solicitud['longitude_destino'] = random.choice(longitudes_finales)
        print(data_solicitud)
        pubsub_class.publish_message(data_solicitud)
        time.sleep(1)


if __name__ == '__main__':
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    pubsub_ofertas = PubSubMessages(args.project_id, args.topic_ofertas)
    pubsub_solicitudes = PubSubMessages(args.project_id, args.topic_solicitudes)

    datos_latitude_total, datos_longitude_total = gen_ofertas(
        NUM_OFERTAS, args.project_id, args.topic_ofertas, args.bucket_name)
    latitudes_finales, longitudes_finales = get_coords_finales(
        args.bucket_name, DOWNLOAD_FOLDER)
    gen_solicitudes(NUM_SOLICITUDES, args.project_id, args.topic_solicitudes,
                    datos_latitude_total, datos_longitude_total, latitudes_finales, longitudes_finales)

    pubsub_ofertas.close()
    pubsub_solicitudes.close()