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
        
        vehiculos = datos[0]
        usuarios = datos[1]
        lista_matches = []
        
        for i in usuarios:
            for e in vehiculos:
                if e['plazas_disponibles'] > 0:
                    if ((i["longitud"] - e["longitud"]) < 6) and ((i["latitud"] - e["latitud"]) < 6):
                        print(f'El usuario: {i["id_usuario"]} ha hecho match con el coche: {e["id_vehiculo"]}')
                        records = {'id_usuario': i["id_usuario"], 'id_vehiculo': e["id_vehiculo"], 'latitud': e['latitud'],
                                   'longitud': e['longitud'], 'plazas_disponibles': (e['plazas_disponibles'] - 1), 'match': 'yes'}
                        lista_matches.append(records)
                        e['plazas_disponibles'] =- 1
                        
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
        vehiculos = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_ofertas}')
            | "Decode msg" >> beam.Map(decode_message)
            | "Window1" >> beam.WindowInto(beam.window.FixedWindows(20))
            | "Asignar clave"  >> beam.ParDo(AssignNumericKey())
    )
        usuarios = (
            p
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_solicitudes}')
            | "Decode msg2" >> beam.Map(decode_message)
            | "Window2" >> beam.WindowInto(beam.window.FixedWindows(20))
            | "Asignar clave 2"  >> beam.ParDo(AssignNumericKey())
    )
        data = (((vehiculos,usuarios)) | beam.CoGroupByKey()
                | beam.ParDo(CheckCoordinatesDoFn())
                | beam.Map(print))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()