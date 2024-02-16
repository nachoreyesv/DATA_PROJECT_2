import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

project_id = "dataflow-clase"
subscription_name_ofertas = "ofertas-final-sub"
subscription_name_solicitudes = "solicitudes-final-sub"
bq_dataset = "dp2"
bq_table = "matchesfinales"
bucket_name = "temp-bucket-dataflow-dp2"

def decode_message(msg):
    output = msg.decode('utf-8')
    #logging.info(output)
    return json.loads(output)

class AssignNumericKey(beam.DoFn):
    def process(self, element):
        yield (element['id_viaje'], element)

class CheckCoordinatesDoFn(beam.DoFn):
    def __init__(self):
        super().__init__()
        self.matched_usuarios_vehiculos = set()

    def process(self, element):
        mensaje_id, datos = element
        vehiculos = datos[0]
        usuarios = datos[1]
        lista_matches = []

        for i in usuarios:
            if i["id_usuario"] in self.matched_usuarios_vehiculos:
                continue  # Saltar el usuario si ya ha sido emparejado

            for e in vehiculos:
                if e['plazas_disponibles'] > 0:
                    if ((i["longitud"] - e["longitud"]) < 50) and ((i["latitud"] - e["latitud"]) < 6):
                        records = {
                            'id_viaje': mensaje_id,
                            'id_usuario': i["id_usuario"],
                            'id_vehiculo': e["id_vehiculo"],
                            'latitud': e['latitud'],
                            'longitud': e['longitud'],
                            'plazas_disponibles': (e['plazas_disponibles'] - 1),
                            'match': 'yes'
                        }
                        lista_matches.append(records)
                        e['plazas_disponibles'] -= 1  # Reducir las plazas disponibles del vehículo

                        # Marcar el usuario como emparejado
                        self.matched_usuarios_vehiculos.add(i["id_usuario"])
                        break  # Salir del bucle interno cuando se encuentra una coincidencia

        # Emitir la lista de coincidencias
        yield lista_matches


class ListtoDict(beam.DoFn):
    def process(self, element):
        for item in element:
            yield item
                       

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
            | beam.ParDo(ListtoDict())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=f"{project_id}:{bq_dataset}.{bq_table}",
            schema="id_viaje:INTEGER, id_usuario:INTEGER, id_vehiculo:INTEGER, latitud:FLOAT, longitud:FLOAT, plazas_disponibles:INTEGER, match:STRING",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            ))
        
                

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()