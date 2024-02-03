from google.cloud import pubsub_v1
import os
import shutil
import random
import xml.etree.ElementTree as ET
import json

# Nombre de tu tema de Pub/Sub
# Reemplaza 'tu-proyecto' con el ID de tu proyecto en Google Cloud
# y 'DATAPROJECT2' con el nombre de tu tema de Pub/Sub
topic_name = 'projects/entregable-cloud-413009/topics/DATAPROJECT2'

# Crea un cliente de Pub/Sub
publisher = pubsub_v1.PublisherClient()

# Carpeta donde están ubicados los archivos KML
# Puedes poner la ruta completa o relativa desde el directorio actual
KML_FOLDER = 'C:/Users/EDEM/Downloads/data_project_2/coordenadas'

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
            # Publica los datos en Pub/Sub
            publish_message(data)

def publish_message(data):
    data_str = json.dumps(data)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_name, data=data_bytes)
    print(f"Mensaje publicado en Pub/Sub: {data_str}")
    future.result()

def gen_ofertas(num_ofertas):
    for i in range(1, num_ofertas + 1):
        kml_file = os.path.join(KML_FOLDER, f'{i}.kml')  # Nombre de tus archivos KML
        read_kml(oferta=i, kml_file=kml_file)

if __name__ == '__main__':
    # Número de ofertas que deseas procesar
    NUM_OFERTAS = 3  
    gen_ofertas(NUM_OFERTAS)
