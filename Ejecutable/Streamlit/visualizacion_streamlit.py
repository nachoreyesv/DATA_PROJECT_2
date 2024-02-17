import random
import pandas as pd
from pandas_gbq import read_gbq
import streamlit as st
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go

project_id = 'dataflow-clase'

query = 'SELECT * FROM `data_project_2.tabla_matches_temp`'
matches_df = read_gbq(query, project_id=project_id, dialect='standard')
matches_df = matches_df.sort_values(by=['id_vehiculo', 'id_usuario'])

query_vehiculos = 'SELECT * FROM `data_project_2.tabla_vehiculos_temp`'    
vehiculos_totales_df = read_gbq(query_vehiculos, project_id=project_id, dialect='standard')
vehiculos_totales_df = vehiculos_totales_df.sort_values(by=['id_vehiculo', 'punto'])

query_usuarios = 'SELECT * FROM `data_project_2.tabla_usuarios_temp`'
usuarios_totales_df = read_gbq(query_usuarios, project_id=project_id, dialect='standard')
usuarios_totales_df = usuarios_totales_df.sort_values(by='id_usuario')

def crear_variables_usuarios():
    nombres_hombre = ["Alejandro", "Pablo", "David", "Javier", "Daniel", "Carlos", "Diego", "Sergio", "Adrian", "Juan", "Miguel", "Jorge", "Antonio", "Francisco", "Manuel", "Guillermo", "Ruben", "Alfonso"]
    nombres_mujer = ["Lucia", "Marta", "Andrea", "Elena", "Ana", "Carmen", "Maria", "Laura", "Sofia", "Sara", "Paula", "Clara", "Raquel", "Natalia", "Martina", "Patricia", "Cristina", "Beatriz", "Isabel"]

    apellidos_españoles = [
        "Garcia", "Fernandez", "Gonzalez", "Martinez", "Rodriguez",
        "Lopez", "Sanchez", "Perez", "Gomez", "Martin",
        "Jimenez", "Ruiz", "Hernandez", "Diaz", "Alvarez",
        "Moreno", "Munoz", "Romero", "Navarro", "Rubio",
        "Molina", "Delgado", "Suarez", "Torres", "Dominguez",
        "Vazquez", "Blanco", "Ramos", "Castro", "Serrano",
        "Ortiz", "Vega", "Flores", "Cabrera", "Medina",
        "Leon", "Prieto", "Pascual", "Bravo", "Arias"
    ]

    data = []
    for _ in range(70):
        id = None
        nombre = None
        apellido = random.choice(apellidos_españoles)
        data.append([id, nombre, apellido])

    usuarios_df = pd.DataFrame(data, columns=['id_usuario', 'nombre', 'apellido'])
    usuarios_df.index = usuarios_df.index + 1
    usuarios_df['id_usuario'] = usuarios_df.index


    edades = ['16-25', '26-35', '36-45', '46-55', '56-65', '>65']
    usuarios_df['edad'] = usuarios_df.apply(lambda x: random.choice(edades), axis = 1) 

    
    def asignar_calificacion_usuario():
        probabilidad = random.random()  
        
        if probabilidad < 0.05: 
            return '1'
        elif probabilidad < 0.1:  
            return '2'
        elif probabilidad < 0.15:
            return '3'
        elif probabilidad < 0.95:  
            return '4'
        else:  
            return '5'
        
    usuarios_df['calificacion_usuario'] = usuarios_df.apply(lambda x: asignar_calificacion_usuario(), axis=1) 


    metodo_pago = ['tarjeta', 'efectivo']
    probabilidad_tarjeta = 0.8 
    usuarios_df['metodo_pago'] = [random.choice(metodo_pago) if random.random() < probabilidad_tarjeta else 'efectivo' for _ in range(len(usuarios_df))]


    def asignar_sexo():
        probabilidad = random.random()
        
        if probabilidad < 0.46:
            return 'mujer'
        else:
            return 'hombre'
        
    usuarios_df['sexo'] = usuarios_df.apply(lambda x: asignar_sexo(), axis=1)


    for i in usuarios_df.index:
        if usuarios_df['sexo'][i] == 'hombre':
            usuarios_df['nombre'][i] = random.choice(nombres_hombre)
        else:
            usuarios_df['nombre'][i] = random.choice(nombres_mujer)


    trabajos = ["Médico", "Ingeniero", "Profesor", "Abogado", "Programador", "Diseñador gráfico", "Enfermero/a", "Contador/a", "Gerente de ventas", "Electricista", "Carpintero", "Chef", "Peluquero/a", "Periodista", "Agricultor/a"]
    usuarios_df['trabajo'] = usuarios_df.apply(lambda x: random.choice(trabajos), axis = 1)


    barrios_valencia = ["Ciutat Vella", "Ensanche", "Extramurs", "Campanar", "La Saidia", "Pla del Real", "Olivereta", "Patraix", "Jesús", "Quatre Carreres", "Poblados del Sur", "Camins al Grau", "Algirós", "Benimaclet", "Rascanya", "Benicalap", "Poblados del Norte", "Poblats Marítims", "Marchalenes", "La Zaidia", "Morvedre", "Tendetes", "Soternes", "La Creu Coberta", "Fuensanta", "Mislata", "Xirivella", "Alboraia", "Burjasot", "Paterna", "Torrent", "Campanar", "Mestalla", "Malvarrosa", "El Cabañal", "Benimaclet", "La Petxina", "El Carme", "El Grau"]
    usuarios_df['direccion'] = usuarios_df.apply(lambda x: random.choice(barrios_valencia), axis = 1)


    años = []
    for i in range(70):
        año_rand = random.randint(2006, 2024)
        años.append(año_rand)
        años.sort()

    usuarios_df['año_registro'] = años

    
    usuarios_df['numero_viajes_anteriores'] = usuarios_df.apply(lambda x: random.randint(0, 50), axis = 1)

    
    preferencias = ['silencio total', 'contar su vida', 'charla sin mas', 'un poco de karaoke']
    usuarios_df['preferencia_viaje'] = usuarios_df.apply(lambda x: random.choice(preferencias), axis = 1)

    
    usuarios_df['vehiculo_propio'] = usuarios_df.apply(lambda x: random.choice([True, False]), axis = 1)

    return usuarios_df

def crear_variables_vehiculos():
    filas = []

    for i in range(30):
        nueva_fila = {
            'id_vehiculo': i,
            'plazas_disponibles': None,
            'calificacion_conductor': None,
            'marca': None,
            'modelo': None,
            'año_de_fabricacion': None,
            'tipo_de_combustible': None,
            'potencia': None,
            'kilometraje': None,
            'estado_del_vehiculo': None,
            'fecha_de_venta': None,
            'accidentes_anteriores': None,
            'genero_conductor': None
        }

        filas.append(nueva_fila)

    vehiculos_df = pd.DataFrame(filas)
    vehiculos_df.index = vehiculos_df.index + 1
    vehiculos_df['id_vehiculo'] = vehiculos_df.index
    
    
    vehiculos_df['plazas_disponibles'] = [3,3,2,4,2,3,2,2,3,4,3,2,2,3,2,2,4,4,3,3,3,4,3,1,1,4,4,4,4,2]


    def asignar_calificacion():
        probabilidad = random.random()  
        
        if probabilidad < 0.05: 
            return '1'
        elif probabilidad < 0.1:  
            return '2'
        elif probabilidad < 0.15: 
            return '3'
        elif probabilidad < 0.95:  
            return '4'
        else:  
            return '5'
        
    vehiculos_df['calificacion_conductor'] = vehiculos_df.apply(lambda x: asignar_calificacion(), axis=1)

    
    def asignar_genero_conductor():
        probabilidad = random.random()
        
        if probabilidad < 0.47:
            return 'mujer'
        else:
            return 'hombre'

    vehiculos_df['genero_conductor'] = vehiculos_df.apply(lambda x: asignar_genero_conductor(), axis=1)


    marcas_coches = ["Toyota", "Ford", "Chevrolet", "Honda", "Volkswagen", "BMW", "Audi", "Mercedes-Benz", "Nissan", "Hyundai", "Kia", "Volvo"]
    vehiculos_df['marca'] = vehiculos_df.apply(lambda x: random.choice(marcas_coches), axis = 1)
    

    for index, row in vehiculos_df.iterrows():
        marca = row['marca']
        if marca in marcas_coches:
            if marca == "Toyota":
                modelo = random.choice(["Camry", "Corolla", "RAV4"])
            elif marca == "Ford":
                modelo = random.choice(["F-150", "Mustang", "Focus"])
            elif marca == "Chevrolet":
                modelo = random.choice(["Silverado", "Camaro", "Malibu"])
            elif marca == "Honda":
                modelo = random.choice(["Civic", "Accord", "CR-V"])
            elif marca == "Volkswagen":
                modelo = random.choice(["Golf", "Passat", "Tiguan"])
            elif marca == "BMW":
                modelo = random.choice(["3 Series", "5 Series", "X5"])
            elif marca == "Audi":
                modelo = random.choice(["A4", "Q5", "A6"])
            elif marca == "Mercedes-Benz":
                modelo = random.choice(["C-Class", "E-Class", "GLC"])
            elif marca == "Nissan":
                modelo = random.choice(["Altima", "Sentra", "Rogue"])
            elif marca == "Hyundai":
                modelo = random.choice(["Sonata", "Elantra", "Tucson"])
            elif marca == "Kia":
                modelo = random.choice(["Optima", "Sorento", "Sportage"])
            elif marca == "Volvo":
                modelo = random.choice(["XC90", "S60", "V60"])
        else:
            modelo = None

        vehiculos_df.at[index, 'modelo'] = modelo


    años_f = []
    for i in range(30):
        años_fab = random.randint(1990, 2020)
        años_f.append(años_fab)

    vehiculos_df['año_de_fabricacion'] = vehiculos_df.apply(lambda x: random.choice(años_f), axis = 1)


    def asignar_combustible():
        probabilidad = random.random()  
        
        if probabilidad < 0.50: 
            return 'gasolina'
        elif probabilidad < 0.70:  
            return 'diesel'
        elif probabilidad < 0.85:
            return 'electrico'
        else:  
            return 'glp'

    vehiculos_df['tipo_de_combustible'] = vehiculos_df.apply(lambda x: asignar_combustible(), axis=1)


    rangos_potencia = [
        "0-100 CV",
        "101-150 CV",
        "151-200 CV",
        "201-250 CV",
        "251-300 CV",
        "301-350 CV",
        "351-400 CV",
        "401-450 CV",
        "451-500 CV",
        "Más de 500 CV"
    ]
    vehiculos_df['potencia'] = vehiculos_df.apply(lambda x: random.choice(rangos_potencia), axis = 1)


    vehiculos_df['kilometraje'] = vehiculos_df.apply(lambda x: random.randint(10000, 100000), axis = 1)


    estados = [
        'nuevo', 'perfecto', 'medio', 'gastado', 'deprorable'
    ]
    vehiculos_df['estado_del_vehiculo'] = vehiculos_df.apply(lambda x: random.choice(estados), axis = 1)


    años_v = []
    for i in range(30):
        años_ven = random.randint(1990, 2020)
        años_v.append(años_ven)

    vehiculos_df['fecha_de_venta'] = vehiculos_df.apply(lambda x: random.choice(años_v), axis = 1)


    for i in vehiculos_df.index:
        if vehiculos_df['año_de_fabricacion'][i] > vehiculos_df['fecha_de_venta'][i]:
            vehiculos_df['fecha_de_venta'][i] = vehiculos_df['año_de_fabricacion'][i]
        else:
            vehiculos_df['fecha_de_venta'][i] = vehiculos_df['fecha_de_venta'][i]

    vehiculos_df['accidentes_anteriores'] = vehiculos_df.apply(lambda x: random.choice([True, False]), axis = 1)

    return vehiculos_df

def mergear_dfs(usuarios_df, vehiculos_df):
    usuarios_matcheados_df = usuarios_df.merge(matches_df['id_usuario'], on='id_usuario', how='left')
    vehiculos_matcheados_df = vehiculos_df.merge(matches_df['id_vehiculo'], on='id_vehiculo', how='left')

    merge1_df = usuarios_df.merge(matches_df[['id_usuario', 'id_vehiculo']], on='id_usuario', how='left')
    usuarios_y_vehiculos_df = merge1_df.merge(vehiculos_df, on='id_vehiculo', how='left')

    return usuarios_matcheados_df, vehiculos_matcheados_df, usuarios_y_vehiculos_df
    

def panel_streamlit(vehiculos_matcheados_df, usuarios_matcheados_df):

    # GRAFICOS ANALISIS CONJUNTO
    
    folium_map = folium.Map(location=[vehiculos_totales_df['latitud'].mean(), vehiculos_totales_df['longitud'].mean()], tiles="openstreetmap", zoom_start=14)
    ids_vehiculo = vehiculos_totales_df['id_vehiculo'].unique()

    colores_vehiculos = ["#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#00FFFF", "#FFA500", "#800080", "#008000", "#808000", "#800000", "#808080", "#FFC0CB", "#FFD700", "#A52A2A", "#B8860B", "#D2691E", "#FF8C00", "#8B0000", "#7FFF00", "#008080", "#000080", "#FF4500", "#4B0082", "#2E8B57", "#DAA520", "#8A2BE2", "#9932CC", "#20B2AA", "#00FF7F"]
    color_usuarios = 'green'

    for i, id_vehiculo in enumerate(ids_vehiculo):
        df_vehiculo = vehiculos_totales_df[vehiculos_totales_df['id_vehiculo'] == id_vehiculo]
        locations = df_vehiculo[['latitud', 'longitud']].values.tolist()

        folium.PolyLine(
            locations=locations,
            color=colores_vehiculos[i % len(colores_vehiculos)], 
            weight=2.5,
            opacity=1
        ).add_to(folium_map)

        vehiculo_inicio = df_vehiculo.iloc[0]
        folium.Marker(
            location=[vehiculo_inicio['latitud'], vehiculo_inicio['longitud']],
            popup=f'Inicio Viaje Nº {vehiculo_inicio["id_viaje"]}',
            icon=folium.Icon(color='magenta', icon="fa-car", prefix='fa')
        ).add_to(folium_map)


    for _, row in usuarios_totales_df.iterrows():
        folium.CircleMarker(
            location=[row['latitud'], row['longitud']],
            popup=f'Usuario Nº {row["id_usuario"]}',
            radius=3,
            color=color_usuarios,
            fill=True,
            fill_color=color_usuarios
        ).add_to(folium_map)

    st.title('Análisis Conjunto de los Vehiculos y Usuarios Matcheados')
    image0 = open('imagenes_streamlit/recogida.jpeg', 'rb').read()
    st.image(image0)

    st.subheader('Mapa Interactivo con Vehiculos y Usuarios')
    folium_static(folium_map)

    st.subheader('Elección del tipo de coche en función del tipo de viaje deseado')
    fig_1 = px.histogram(usuarios_y_vehiculos_df, x='marca', color='preferencia_viaje', 
                         labels={'marca': 'Marca del vehiculo', 'preferencia_viaje': 'Preferencia de Tipo de Viaje del usuario'})
    fig_1.update_layout(yaxis_title='Cantidad de usuarios')
    st.plotly_chart(fig_1)

    st.subheader('Preferencia del tipo de conductor en función de la edad del usuario')
    fig_2 = px.violin(usuarios_y_vehiculos_df, x='edad', y='genero_conductor', 
                     labels={'edad': 'Edad del usuario', 'genero_conductor': 'Genero del conductor'})
    st.plotly_chart(fig_2)

    st.subheader('Valoraciones a los conductores por el genero de los usuarios')
    fig_3 = px.bar(usuarios_y_vehiculos_df, 
             x='calificacion_conductor', 
             color='sexo',
             labels={'calificacion_conductor': 'Calificación del Conductor', 'sexo': 'Genero del usuario'},
             title='Calificación de Conductores por Sexo'
            )
    fig_3.update_layout(yaxis_title='Cantidad de usuarios')
    st.plotly_chart(fig_3)

    # GRAFICOS ANALISIS VEHICULOS SOLO
    st.title('Análisis de los Vehiculos Matcheados')
    image = open('imagenes_streamlit/analisis_coches.jpeg', 'rb').read()
    st.image(image)

    st.subheader('Número de vehículos por marca')
    fig1 = px.bar(vehiculos_matcheados_df['marca'].value_counts(), x=vehiculos_matcheados_df['marca'].value_counts().index,
                   y=vehiculos_matcheados_df['marca'].value_counts().values, labels= {'x': 'Marca', 'y': 'Número de vehículos'})
    st.plotly_chart(fig1)

    st.subheader('Calificación promedio por marca y modelo de coche')
    fig2 = px.bar(vehiculos_matcheados_df, x='marca', y='calificacion_conductor', color='modelo', labels= {'marca': 'Marca', 'calificacion_conductor': 'Calificacion Conductor', 
                                                                                                           'modelo': 'Modelo del vehiculo'})
    st.plotly_chart(fig2)

    st.subheader('Distribución del tipo de combustible')
    fig3 = px.pie(vehiculos_matcheados_df, names='tipo_de_combustible', labels={'tipo_de_combustible': 'Tipo de Combustible'})
    st.plotly_chart(fig3)

    st.subheader('Relación entre el estado del vehículo y accidentes anteriores')
    vehiculos_matcheados_df['accidentes_anteriores'] = vehiculos_matcheados_df['accidentes_anteriores'].astype('category')
    colors = {'medio': 'blue', 'perfecto': 'green', 'deprorable': 'red', 'gastado': 'orange', 'nuevo': 'white'}
    fig4 = px.histogram(vehiculos_matcheados_df, x='estado_del_vehiculo', color='estado_del_vehiculo', 
                    facet_col='accidentes_anteriores', color_discrete_map=colors, labels={'accidentes_anteriores': 'Vehiculo con accidente anterior', 
                                                                                          'estado_del_vehiculo': 'Estado del Vehiculo'})
    fig4.update_xaxes(title_text="Estado del vehículo")
    fig4.update_yaxes(title_text="Cantidad", row=1, col=1)
    fig4.update_yaxes(title_text="Cantidad", row=1, col=2)
    st.plotly_chart(fig4)

    st.subheader('Relación entre potencia y kilometraje')
    fig5 = px.scatter(vehiculos_matcheados_df, x='potencia', y='kilometraje', labels= {'potencia': 'Potencia del vehículo', 'kilometraje': 'Kilómetros'})
    st.plotly_chart(fig5)

    st.subheader('Distribución del kilometraje por año de fabricación')
    fig6 = px.box(vehiculos_matcheados_df, x='año_de_fabricacion', y='kilometraje', points="all", labels= {'año_de_fabricacion': 'Año de fabricación del vehículo', 'kilometraje': 'Kilómetros'})
    st.plotly_chart(fig6)

    # GRAFICOS ANALISIS USUARIOS SOLO
    st.title('Análisis de los Usuarios Matcheados')
    image0 = open('imagenes_streamlit/analisis_usuarios.jpeg', 'rb').read()
    st.image(image0)

    st.subheader('Preferencia del tipo de viaje según la edad del usuario')
    fig01 = px.histogram(usuarios_matcheados_df, x='edad', color='preferencia_viaje', labels= {'edad': 'Edad del usuario', 'preferencia_viaje': 'Tipos de Viaje', 'count': 'Cantidad de usuarios'})
    fig01.update_layout(yaxis_title='Cantidad de usuarios')
    st.plotly_chart(fig01)

    st.subheader('Porcentaje de pagos en efectivo y tarjeta')
    fig02 = px.pie(usuarios_matcheados_df, names='metodo_pago', labels={'metodo_pago': 'Metodo de pago del viaje'})
    fig02.update_layout(
    legend=dict(title='Método de pago del viaje'))
    st.plotly_chart(fig02)

    st.subheader('Numero de viajes dependiendo de la dirección de los usuarios')
    viajes_por_direccion = usuarios_matcheados_df.groupby('direccion')['numero_viajes_anteriores'].sum().reset_index()
    fig03 = px.bar(viajes_por_direccion, x='direccion', y='numero_viajes_anteriores')
    fig03.update_layout(xaxis_title='Dirección', yaxis_title='Número de Viajes Anteriores')
    st.plotly_chart(fig03)

    st.subheader('Año de registro por sexo del usuario')
    fig04 = px.violin(usuarios_matcheados_df, x="sexo", y="año_registro", color="sexo", box=True,
                labels={'sexo': 'Sexo del usuario', 'año_registro': 'Año del registro'},
                violinmode='overlay')
    fig04.update_traces(meanline_visible=True, jitter=0.05, scalemode='count', width=0.8,
                  marker=dict(color='blue'), line=dict(color='blue')) 
    fig04.update_traces(marker=dict(color='red'), line=dict(color='red'), selector=dict(name='hombre'))
    st.plotly_chart(fig04)

    fig05 = px.bar(usuarios_matcheados_df, 
               x='edad', 
               color='vehiculo_propio',
               labels={'vehiculo_propio': 'Posee vehiculo propio el usuario'})
    fig05.update_layout(xaxis_title='Edad de los usuarios', yaxis_title='Cantidad de Usuarios')
    st.plotly_chart(fig05)

    fig06 = px.box(usuarios_matcheados_df, 
               x='trabajo', 
               y='calificacion_usuario', 
               color='trabajo',
               labels={'trabajo': 'Tipo de Trabajo', 'calificacion_usuario': 'Puntuación del usuario'}
              )
    fig06.update_layout(xaxis_title='Tipo de Trabajo', yaxis_title='Puntuación del usuario')
    st.plotly_chart(fig06)

if __name__ == "__main__":
    usuarios_df = crear_variables_usuarios()
    vehiculos_df = crear_variables_vehiculos()
    usuarios_matcheados_df, vehiculos_matcheados_df, usuarios_y_vehiculos_df = mergear_dfs(usuarios_df, vehiculos_df)
    panel_streamlit(vehiculos_matcheados_df = vehiculos_matcheados_df, usuarios_matcheados_df = usuarios_matcheados_df)
