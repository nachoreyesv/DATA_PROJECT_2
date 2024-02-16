import pandas as pd
import random

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

df = pd.DataFrame(data, columns=['id_usuario', 'nombre', 'apellido'])
df.index = df.index + 1
df['id_usuario'] = df.index

edades = ['16-25', '26-35', '36-45', '46-55', '56-65', '>65']
df['edad'] = df.apply(lambda x: random.choice(edades), axis = 1) 

sexo = ['hombre', 'mujer']
df['sexo'] = df.apply(lambda x: random.choice(sexo), axis = 1)

for i in df.index:
  if df['sexo'][i] == 'hombre':
    df['nombre'][i] = random.choice(nombres_hombre)
  else:
    df['nombre'][i] = random.choice(nombres_mujer)

trabajos = ["Médico", "Ingeniero", "Profesor", "Abogado", "Programador", "Diseñador gráfico", "Enfermero/a", "Contador/a", "Gerente de ventas", "Electricista", "Carpintero", "Chef", "Peluquero/a", "Periodista", "Agricultor/a"]
df['trabajo'] = df.apply(lambda x: random.choice(trabajos), axis = 1)

barrios_valencia = ["Ciutat Vella", "Ensanche", "Extramurs", "Campanar", "La Saidia", "Pla del Real", "Olivereta", "Patraix", "Jesús", "Quatre Carreres", "Poblados del Sur", "Camins al Grau", "Algirós", "Benimaclet", "Rascanya", "Benicalap", "Poblados del Norte", "Poblats Marítims", "Marchalenes", "La Zaidia", "Morvedre", "Tendetes", "Soternes", "La Creu Coberta", "Fuensanta", "Mislata", "Xirivella", "Alboraia", "Burjasot", "Paterna", "Torrent", "Campanar", "Mestalla", "Malvarrosa", "El Cabañal", "Benimaclet", "La Petxina", "El Carme", "El Grau"]
df['direccion'] = df.apply(lambda x: random.choice(barrios_valencia), axis = 1)

años = []
for i in range(70):
  año_rand = random.randint(2006, 2024)
  años.append(año_rand)
años.sort()
df['año_registro'] = años

df['numero_viajes_anteriores'] = df.apply(lambda x: random.randint(0, 50), axis = 1)

preferencias = ['silencio total', 'contar su vida', 'charla sin mas', 'un poco de karaoke']
df['preferencia_viaje'] = df.apply(lambda x: random.choice(preferencias), axis = 1)

df['vehiculo_propio'] = df.apply(lambda x: random.choice([True, False]), axis = 1)

import pandas as pd

data = {
    'id_vehiculo': [],
    'plazas_disponibles': [],
    'marca': [],
    'modelo': [],
    'año_de_fabricacion': [],
    'tipo_de_combustible': [],
    'potencia': [],
    'kilometraje': [],
    'estado_del_vehiculo': [],
    'fecha_de_venta': [],
    'accidentes_anteriores': []
}

df_vehiculos = pd.DataFrame(data)

for i in range(30):
    nueva_fila = {
        'id_vehiculo': i,
        'plazas_disponibles': None,
        'marca': None,
        'modelo': None,
        'año_de_fabricacion': None,
        'tipo_de_combustible': None,
        'potencia': None,
        'kilometraje': None,
        'estado_del_vehiculo': None,
        'fecha_de_venta': None,
        'accidentes_anteriores': None
    }
    df_vehiculos = df_vehiculos.append(nueva_fila, ignore_index=True)

df_vehiculos.index = df_vehiculos.index + 1

df_vehiculos['id_vehiculo'] = df_vehiculos.index
df_vehiculos['plazas_disponibles'] = [3,3,2,4,2,3,2,2,3,4,3,2,2,3,2,2,4,4,3,3,3,4,3,1,1,4,4,4,4,2]

marcas_coches = ["Toyota", "Ford", "Chevrolet", "Honda", "Volkswagen", "BMW", "Audi", "Mercedes-Benz", "Nissan", "Hyundai", "Kia", "Volvo"]
df_vehiculos['marca'] = df_vehiculos.apply(lambda x: random.choice(marcas_coches), axis = 1)

for index, row in df_vehiculos.iterrows():
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

    df_vehiculos.at[index, 'modelo'] = modelo

años_f = []
for i in range(30):
  años_fab = random.randint(1990, 2020)
  años_f.append(años_fab)

df_vehiculos['año_de_fabricacion'] = df_vehiculos.apply(lambda x: random.choice(años_f), axis = 1)

combustibles = ['gasolina', 'diesel', 'hibrido', 'electrico']
df_vehiculos['tipo_de_combustible'] = df_vehiculos.apply(lambda x: random.choice(combustibles), axis = 1)

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
df_vehiculos['potencia'] = df_vehiculos.apply(lambda x: random.choice(rangos_potencia), axis = 1)

kilometrajes = [
    "0-10.000 km",
    "10.001-20.000 km",
    "20.001-30.000 km",
    "30.001-40.000 km",
    "40.001-50.000 km",
    "50.001-60.000 km",
    "60.001-70.000 km",
    "70.001-80.000 km",
    "80.001-90.000 km",
    "90.001-100.000 km",
    "Más de 100.000 km"
]
df_vehiculos['kilometraje'] = df_vehiculos.apply(lambda x: random.choice(kilometrajes), axis = 1)

estados = [
    'nuevo', 'perfecto', 'medio', 'gastado', 'deprorable'
]
df_vehiculos['estado_del_vehiculo'] = df_vehiculos.apply(lambda x: random.choice(estados), axis = 1)

años_v = []
for i in range(30):
  años_ven = random.randint(1990, 2020)
  años_v.append(años_ven)

df_vehiculos['fecha_de_venta'] = df_vehiculos.apply(lambda x: random.choice(años_v), axis = 1)

for i in df_vehiculos.index:
  if df_vehiculos['año_de_fabricacion'][i] > df_vehiculos['fecha_de_venta'][i]:
    df_vehiculos['fecha_de_venta'][i] = df_vehiculos['año_de_fabricacion'][i]
  else:
    df_vehiculos['fecha_de_venta'][i] = df_vehiculos['fecha_de_venta'][i]

df_vehiculos['accidentes_anteriores'] = df_vehiculos.apply(lambda x: random.choice([True, False]), axis = 1)