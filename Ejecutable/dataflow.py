import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

# Variables
project_id = "dataproject2-413213"
subscription_name_ofertas = "topic_ofert-sub"
subscription_name_solicitudes = "topic_solicit-sub"
bq_dataset = "pruebadata"
bq_table = "ejemplo"
bucket_name = "pruebagp"

def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info(output)

    return json.loads(output)

class OutputDoFn(beam.DoFn):

    def process(self, element):
        lista_vehiculos=[]
        lista_solicitantes=[]
        lista_matches = []
        lista_no_matches = []

        if isinstance(element, dict):
          if "id_oferta" in element:
              lista_vehiculos.append(element)
          else:
              lista_solicitantes.append(element)
        else:
            logging.warning(f"Elemento no es un diccionario: {element}")

        for i in lista_solicitantes:
            for e in lista_vehiculos:
                if ((i["longitude_destino"] == e["longitude_destino"]) & (i["latitude_destino"] == e["latitude_destino"])):
                    if ((i["longitude"]-e["longitude"]) < 0.8) & ((i["latitude"]-e["latitude"]) < 0.8):
                        print(f'El usuario: {i["id_persona"]} ha hecho match con el coche: {e["id_coche"]}')
                        print(i)
                        records = {'id1': i["id_solicitante"], 'id2': e["id_oferta"], 'latitude1': i['latitude'],
                                  'longitude1': i['longitude'], 'latitude2': i['latitude'],
                                  'longitude2': i['longitude'], 'latitude_final1': i['latitude_destino'],
                                  'longitude_final1': i['longitude_destino'], 'latitude_final2': i['latitude_destino'],
                                  'longitude_final2': i['longitude_destino'], 'match': 'yes'}
                        lista_matches.append(records)

                else:
                    print('No se ha hecho match')
                    print(i, e)
                    records = {'id1': i["id_solicitante"], 'id2': e["id_oferta"], 'latitude1': i['latitude'],
                                  'longitude1': i['longitude'], 'latitude2': e['latitude'],
                                  'longitude2': e['longitude'], 'latitude_final1': i['latitude_destino'],
                                  'longitude_final1': i['longitude_destino'], 'latitude_final2': e['latitude_destino'],
                                  'longitude_final2': e['longitude_destino'], 'match': 'no'}
                    lista_no_matches.append(records)

        yield lista_matches, lista_no_matches

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        project=project_id,
        runner="DataflowRunner",
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west6"
    )) as p:
        p1=(
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_ofertas}')
            | "Decode msg" >> beam.Map(decode_message)
            | "Window1" >> beam.WindowInto(beam.window.FixedWindows(30)  # Aplicar ventana a p1
        )


        )
        p2=(
            p 
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_solicitudes}')
            | "Decode msg2" >> beam.Map(decode_message)
            | "Window2" >> beam.WindowInto(beam.window.FixedWindows(30))  # Aplicar ventana a p2
        )

        
        data=(p1,p2)
        (
        data
        
            | beam.Flatten()
            |  "Encontrar Matches" >> beam.ParDo(OutputDoFn())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
               table = f"{project_id}:{bq_dataset}.{bq_table}",
               schema="id1:STRING, id2:STRING, latitude1:STRING, longitude1:STRING, latitude2:STRING, longitude2:STRING, latitude_final1:STRING, longitude_final1:STRING, latitude_final2:STRING, longitude_final2:STRING",
               create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
           )
        )
        #data | "print" >> beam.Map(print)



if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    run()
   