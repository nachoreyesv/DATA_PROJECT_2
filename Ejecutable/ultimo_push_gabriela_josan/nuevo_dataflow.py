import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

# Variables
project_id = "dataproject2-413213"
subscription_name_ofertas = "topic_ofertas-sub"
subscription_name_solicitudes = "topic_solicitudes-sub"
bq_dataset = "dataproject2-413213.pruebadata"
bq_table = "dataproject2-413213.pruebadata.ejemplo"
bucket_name = "pruebagp"

def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info("New PubSub Message: %s", output)

    return json.loads(output)

class OutputDoFn(beam.DoFn):

    def process(self, element):
        lista_vehiculos=[]
        lista_solicitantes=[]

        data_lista = element
        for i in data_lista:
            if "id_oferta" in i.keys():
                lista_vehiculos.append(i)
            else:
                lista_solicitantes.append(i)
        

        for i in lista_solicitantes:
            for e in lista_vehiculos:
                if ((i["longitude"]-e["longitude"]) < 5) & ((i["latitude"]-e["latitude"]) < 5):
                    print('match')

                else:
                    print('no match')

        yield lista_solicitantes, lista_vehiculos

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        # save_main_session=True
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
            
        )
        p2=(
            p 
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_solicitudes}')
            | "Decode msg2" >> beam.Map(decode_message)
        )
        data=((p1,p2) | beam.Flatten())
        data | "Fixed Window" >> beam.WindowInto(beam.window.FixedWindows(10))
        data | "Encontrar Matches" >> beam.ParDo(OutputDoFn())

        data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = bq_table, # Required Format: PROJECT_ID:DATASET.TABLE
                schema="latitude:STRING, longitude:STRING, longitude_final:STRING, latitude_final:STRING", # Required Format: field:TYPE
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        data | "print" >> beam.Map(print)



if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")
    
    # Run Process
    run()