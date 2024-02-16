import pandas as pd
import streamlit as st
import random
import folium
from streamlit_folium import folium_static
import time

def gen_ofertas(vehiculos_data, num_rutas):
    res = []
    ofertas = []
    no_rep = list(range(1, 11))

    for i in range(num_rutas):
        a = random.choice(no_rep)
        res.append(a)
        no_rep.remove(a)
    
    for i in res:
        matching_rows = [vehiculo for vehiculo in vehiculos_data if vehiculo['id_viaje'] == i]

        if matching_rows:
            ofertas.append(matching_rows)

    return ofertas

def gen_solicitudes(ofertas):
    solicitudes = []

    for oferta in ofertas:
        puntos_posibles = [vehiculo['punto'] for vehiculo in oferta]
        punto_inicio = random.choice(puntos_posibles[:len(puntos_posibles) // 2])
        indices_validos = [idx for idx, punto in enumerate(puntos_posibles[len(puntos_posibles) // 2:]) if punto - punto_inicio >= 40]

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

def calculo_bla(costes):
    utilidad_blablacar = ["{:.2f}".format(coste * 0.30) for coste in costes]
    return utilidad_blablacar

def calculo_conductor(costes):
    utilidad_conductor = ["{:.2f}".format(coste * 0.70) for coste in costes]
    return utilidad_conductor

def mapa(vehiculos_data, usuarios_data, solicitudes, current_punto):
    all_coordinates = pd.DataFrame(vehiculos_data + usuarios_data)[['latitud', 'longitud']]
    mean_latitude = all_coordinates['latitud'].mean()
    mean_longitude = all_coordinates['longitud'].mean()

    folium_map = folium.Map(location=[mean_latitude, mean_longitude], tiles="cartodb positron", zoom_start=13)

    colors = ['blue', 'purple', 'darkblue', 'green', 'lightgreen', 'orange', 'gray', 'cadetblue', 'lightgray', 'pink', 'lightblue', 'red', 'darkred', 'darkgreen', 'white', 'beige']

    for i, vehiculo in enumerate(vehiculos_data):
        # Icono de inicio de trayecto
        folium.Marker(
            location=(vehiculo['latitud'], vehiculo['longitud']),
            popup=f'Inicio Viaje Nº {vehiculo["id_viaje"]}',
            icon=folium.Icon(color=colors[i], 
                             icon ="fa-car", 
                             prefix = 'fa')
        ).add_to(folium_map)

    for usuario in usuarios_data:
        folium.Marker(
            location=(usuario['latitud'], usuario['longitud']),
            popup=f'Usuario ID: {usuario["id_usuario"]}',
            icon=folium.Icon(color='red', icon='user')
        ).add_to(folium_map)

    for solicitud in solicitudes:
        punto_inicio, punto_final = solicitud
        latitude_inicio, longitude_inicio = vehiculos_data[punto_inicio - 1]['latitud'], vehiculos_data[punto_inicio - 1]['longitud']
        latitude_final, longitude_final = vehiculos_data[punto_final - 1]['latitud'], vehiculos_data[punto_final - 1]['longitud']

        # Marcador solicitud inicio (lugar donde se sube el cliente)
        folium.Marker(
            location=[latitude_inicio, longitude_inicio],
            popup=f'Cliente sube al coche en el punto {punto_inicio}',
            icon=folium.Icon(color='green', icon='user')
        ).add_to(folium_map)

        # Marcador solicitud final (lugar donde se baja el cliente)
        folium.Marker(
            location=[latitude_final, longitude_final],
            popup=f'Cliente baja del coche en el punto {punto_final}',
            icon=folium.Icon(color='red', icon='user')
        ).add_to(folium_map)

    # Icono del coche en movimiento
    if current_punto > 0:
        latitude = vehiculos_data[current_punto - 1]['latitud']
        longitude = vehiculos_data[current_punto - 1]['longitud']
        folium.Marker(
            location=[latitude, longitude],
            icon=folium.Icon(color='darkblue', icon='car', prefix='fa')
        ).add_to(folium_map)

    return folium_map

if __name__ == '__main__':

    st.title('Servicio Blablacar')

    # Dataframes de vehículos y usuarios
    vehiculos_data = [
        {"id_vehiculo": 2, "punto": 218, "longitud": -0.34952, "latitud": 39.45406, "id_viaje": 2},
        {"id_vehiculo": 3, "punto": 219, "longitud": -0.34852, "latitud": 39.45506, "id_viaje": 3},
    ]

    usuarios_data = [
        {"id_usuario": 9, "longitud": -0.35346, "latitud": 39.46327},
        {"id_usuario": 10, "longitud": -0.35446, "latitud": 39.46427},
    ]

    num_rutas = st.selectbox('Número de coches', range(1, 11))
    ofertas = gen_ofertas(vehiculos_data, num_rutas)
    solicitudes = gen_solicitudes(ofertas)
    coste = calcular_coste(solicitudes)
    utilidad = calculo_bla(coste)
    utilidad_conductor = calculo_conductor(coste)
    current_punto = st.empty()
    current_punto.text("El coche está en el punto 0 de la ruta.")
    folium_map = mapa(vehiculos_data, usuarios_data, solicitudes, 0)
    map_component = folium_static(folium_map)

    for punto in range(1, len(vehiculos_data) + 1):
        time.sleep(0.2)
        current_punto.text(f"El coche está en el punto {punto} de la ruta.")
        map_component.empty()
        folium_map = mapa(vehiculos_data, usuarios_data, solicitudes, punto)
        map_component = folium_static(folium_map)

        if punto == len(vehiculos_data):
            break
