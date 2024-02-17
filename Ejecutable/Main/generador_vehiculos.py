import os
import random
import json
import time
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery
import xml.etree.ElementTree as ET
import argparse
import logging
import threading

BASE_URL = 'https://us-central1-dataflow-clase.cloudfunctions.net/main'
NUM_VEHICULOS = 30
DOWNLOAD_FOLDER = 'get_coord'

bigquery_client = bigquery.Client()
#dataset_id = 'data_project_2'
#table_id = 'tabla_vehiculos'

parser = argparse.ArgumentParser(description=('Streaming Data Generator'))

parser.add_argument(
    '--project_id',
    required=True,
    help='GCP cloud project name.')
parser.add_argument(
    '--topic_vehiculos',
    required=True,
    help='PubSub topic de vehiculos.')
parser.add_argument(
    '--bucket_name',
    required=True,
    help='Google Cloud Storage bucket name.')

args, opts = parser.parse_known_args()

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = Blob(source_blob_name, bucket)
    blob.download_to_filename(destination_file_name)

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


def write_to_bigquery(data):

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery_client.get_table(table_ref)

    errors = bigquery_client.insert_rows(table, [data])
    if errors:
        print('Error al insertar fila en BigQuery:', errors)

def read_kml(vehiculo, bucket_name, file_id, project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)

    kml_file = os.path.join(DOWNLOAD_FOLDER, f'{file_id}.kml')
    download_blob(bucket_name, f'{file_id}.kml', kml_file)

    data = {"id_vehiculo": [], "punto": [], "longitud": [], "latitud": [], "id_viaje": None, "plazas_disponibles": None}

    with open(kml_file, "r", encoding="utf-8") as file:
        kml_data = file.read()

    root = ET.fromstring(kml_data)
    coords = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")

    if coords is not None:
        coords_str = coords.text
        coords_list = [tuple(map(float, _.split(',')))[:2] for _ in coords_str.split()]

        data['plazas_disponibles'] = random.randint(1,4)

        for _, coords in enumerate(coords_list):

            data["id_vehiculo"] = vehiculo
            data["punto"] = _ + 1
            data["longitud"] = coords[0]
            data["latitud"] = coords[1]
            data["id_viaje"] = file_id
            print(data)
            pubsub_class.publish_message(data)
            #write_to_bigquery(data)
            time.sleep(5)

def gen_vehiculos(num_vehiculos, project_id, topic_name, bucket_name):
    threads = []
    for i in range(1, num_vehiculos + 1):

        file_id = random.randint(1,27)
        thread = threading.Thread(target=read_kml, args=(i, bucket_name, file_id, project_id, topic_name))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    pubsub_vehiculos = PubSubMessages(args.project_id, args.topic_vehiculos)

    gen_vehiculos(NUM_VEHICULOS, args.project_id, args.topic_vehiculos, args.bucket_name)
    
    pubsub_vehiculos.close()
