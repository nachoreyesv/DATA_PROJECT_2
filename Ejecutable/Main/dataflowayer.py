import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

project_id = "dataflow-clase"
subscription_name_ofertas = "ofertas-final-sub"
subscription_name_solicitudes = "solicitudes-final-sub"
bq_dataset = "dp2"
bq_table = "prueba1502"
bucket_name = "data-flow-bucket-dp2"

def decode_message(msg):
    output = msg.decode('utf-8')
    logging.info(output)
    return json.loads(output)

class AssignNumericKey(beam.DoFn):
    def process(self, element):
        yield (element['id_viaje'], element)



class CheckCoordinatesDoFn(beam.DoFn):
    def process(self, element):
        viaje, (ofertas, solicitudes) = element
        
        
        
        # Generación de Resultados: Estás generando registros de coincidencia (records) y los estás almacenando en una lista (lista_matches). Sin embargo, estás devolviendo lista_matches como un solo elemento en cada iteración. Esto devolverá solo las coincidencias encontradas en el último ciclo. Deberías considerar devolver cada registro de coincidencia individualmente o quizás agruparlos de manera diferente según tus necesidades.
        lista_matches = []
        for i in solicitudes:
            for e in ofertas:
                
                if (i["longitud_destino"] == e["longitud_destino"]) and (i["latitud_destino"] == e["latitud_destino"]):
                    # if ((i["longitud"] - e["longitud"]) <0.00001) and ((i["latitud"] - e["latitud"]) < 0.00001):
                    if ((i["longitud"] - e["longitud"]) <0.000001) and ((i["latitud"] - e["latitud"]) < 0.000001):
                    # if ((i["longitud"] - e["longitud"]) <0.000015) and ((i["latitud"] - e["latitud"]) < 0.000015): #con esta medida me daban match y no match la cambio para que todos me den match

                        records = {'id_usuario':i["id_usuario"],
                                    'id_vehiculo':e["id_vehiculo"],
                                    'latitud_solicitante': i['latitud'],
                                    'longitud_solicitante': i['longitud'],
                                    'latitud_vehiculo': i['latitud'],
                                    'longitud_vehiculo': i['longitud'],
                                    'latitud_final_solicitante': i['latitud_destino'],
                                    'longitud_final_solicitante': i['longitud_destino'], 'latitud_final_vehiculo': i['latitud_destino'],
                                    'longitud_final_vehiculo': i['longitud_destino'], 'match': 'yes'}
                        
                        lista_matches.append(records)

            if len(lista_matches)>0:
                for match in lista_matches:
                    yield match   
            else:
                    yield "no match"
        #         data = {
#             "id_oferta": str(element["id_oferta"]),
#             "punto": str(element["punto"]),
#             "longitude": str(element["longitude"]),
#             "latitude": str(element["latitude"]),
#             "longitude_destino": str(element["longitude_destino"]),
#             "latitude_destino": str(element["latitude_destino"]),
#             "id_viaje": str(element["id_viaje"])
#         }
        
#         yield data

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
            | "Window1" >> beam.WindowInto(beam.window.FixedWindows(10))
            | "Asignar clave"  >> beam.ParDo(AssignNumericKey())
            # | "Imprimir1">>beam.Map(print)
    )
        solicitudes = (
            p
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_solicitudes}')
            | "Decode msg2" >> beam.Map(decode_message)
            | "Window2" >> beam.WindowInto(beam.window.FixedWindows(10))
            | "Asignar clave 2"  >> beam.ParDo(AssignNumericKey()))
            # | "Imprimir2">>beam.Map(print))

        data = (
            (ofertas, solicitudes)
            | "Merge PCollections" >> beam.CoGroupByKey()
            | beam.ParDo(CheckCoordinatesDoFn())
            | "Imp">> beam.Map(print)
        )
    
        # data = (((ofertas,solicitudes)) | beam.CoGroupByKey()
        #     | beam.Map(print))
                
        
        # | beam.ParDo(CheckCoordinatesDoFn())
            # | "Adaptar mensaje a BigQuery" >> beam.ParDo(OutputDoFn()))
#             | "Write to BigQuery" >> beam.io.WriteToBigQuery(
#             table=f"{project_id}:{bq_dataset}.{bq_table}",
#             schema="id_oferta:STRING, punto:STRING, longitude:STRING, latitude:STRING, longitude_destino:STRING, latitude_destino:STRING, id_viaje:STRING",
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
#             )
# )


if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)
    # logging.info("The process started")
    run()
