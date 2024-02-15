import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

project_id = "dataflow-clase"
subscription_name_ofertas = "ofertas-final-sub"
subscription_name_solicitudes = "solicitudes-final-sub"
bq_dataset = "dp2"
bq_table = "dp2-table-new"
bucket_name = "temp-bucket-dataflow-dp2"

def decode_message(msg):
    output = msg.decode('utf-8')
    #logging.info(output)
    return json.loads(output)

class AssignNumericKey(beam.DoFn):
    def process(self, element):
        yield (element['id_viaje'], element)

class CheckCoordinatesDoFn(beam.DoFn):
    def process(self, element):
        mensaje_id, datos = element
        
        ofertas = datos['ofertas']
        solicitudes = datos['solicitudes']
        lista_matches = []
        
        for i in solicitudes:
            for e in ofertas:
                if (i["longitude_destino"] == e["longitude_destino"]) and (i["latitude_destino"] == e["latitude_destino"]):
                    if ((i["longitude"] - e["longitude"]) < 6) and ((i["latitude"] - e["latitude"]) < 6):
                        print(f'El usuario: {i["id_solicitud"]} ha hecho match con el coche: {e["id_oferta"]}')
                        records = {'id_solicitante': i["id_solicitud"], 'id_vehiculo': e["id_oferta"], 'latitud_solicitante': i['latitude'],
                                        'longitud_solicitante': i['longitude'], 'latitud_vehiculo': i['latitude'],
                                        'longitud_vehiculo': i['longitude'], 'latitud_final_solicitante': i['latitude_destino'],
                                        'longitud_final_solicitante': i['longitude_destino'], 'latitud_final_vehiculo': i['latitude_destino'],
                                        'longitud_final_vehciulo': i['longitude_destino'], 'match': 'yes'}
                        lista_matches.append(records)
        # Devolver una lista que contiene todas las coincidencias
        yield lista_matches

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        project=project_id,
        runner="DirectRunner",
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west6"
    )) as p:    
        ofertas = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_ofertas}')
            | "Decode msg" >> beam.Map(decode_message)
            | "Window1" >> beam.WindowInto(beam.window.FixedWindows(20))
            | "Asignar clave"  >> beam.ParDo(AssignNumericKey())
    )
        solicitudes = (
            p
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_solicitudes}')
            | "Decode msg2" >> beam.Map(decode_message)
            | "Window2" >> beam.WindowInto(beam.window.FixedWindows(20))
            | "Asignar clave 2"  >> beam.ParDo(AssignNumericKey())
    )
        data = (((ofertas,solicitudes)) | beam.CoGroupByKey()
                | beam.ParDo(CheckCoordinatesDoFn())
                | beam.Map(print))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


#with open('ids_solicitantes_seleccionados.txt', 'r') as file:
#            ids = file.readlines()
#            lista_ids_solicitantes = [eval(i.strip()) for i in ids]

#ids_lista_solicitantes = {i['id_solicitud'] for i in lista_ids_solicitantes} 
#lista_solicitantes = [i for i in lista_solicitantes if i['id_solicitud'] not in ids_lista_solicitantes]

#for i in lista_matches:
#     lista_ids_solicitantes.append(i['id_solcitante'])

#with open('ids_solicitantes_seleccionados.txt', 'w') as file:
#            for id_solicitante in lista_ids_solicitantes:
#                file.write(id_solicitante + '\n')