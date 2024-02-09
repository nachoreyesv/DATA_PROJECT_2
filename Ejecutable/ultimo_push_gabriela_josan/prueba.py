import os
import xml.etree.ElementTree as ET
import random
import time

def read_kml(oferta, file_path):
    data = {"id_oferta": [], "punto": [], "latitude": [], "longitude": [], "Coordinates":[]}
    datos_longitude = []
    datos_latitude = []

    with open(file_path, "r", encoding="utf-8") as file:
        kml_data = file.read()
        print(kml_data)

    root = ET.fromstring(kml_data)
    coords = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")

    if coords is not None:
        coords_str = coords.text
        coords_list = [tuple(map(float, _.split(','))) for _ in coords_str.split()]

        for i, coords in enumerate(coords_list):
            data["id_oferta"].append(oferta)
            data["punto"].append(i + 1)
            data["latitude"].append(coords[1])
            data["longitude"].append(coords[0])
            datos_latitude.append(coords[1])
            datos_longitude.append(coords[0])
            data["coordinates"].append((coords[1], coords[0]))

    return datos_longitude, datos_latitude

read_kml(2,"C:/Users/josan/Documents/GitHub/DATA_PROJECT_2/Ejecutable/coordenadas")