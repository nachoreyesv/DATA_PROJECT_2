import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

project_id = "dataflow-clase"
subscription_name_ofertas = "oferta-sub"
subscription_name_solicitudes = "sol-sub"
bq_dataset = "dp2"
bq_table = "dp2-table-new"
bucket_name = "temp-bucket-dataflow-dp2"

def decode_message(msg):
    output = msg.decode('utf-8')
    logging.info(output)
    return json.loads(output)

class OutputDoFn(beam.DoFn):
    def process(self, element):
        lista_vehiculos = []
        lista_solicitantes = []
        lista_matches = []
        lista_no_matches = []
        lista_ids_solicitantes = []

        if "id_oferta" in element.keys():
            lista_vehiculos.append(element)
        else:
            lista_solicitantes.append(element)

        with open('ids_solicitantes.txt', 'r') as file:
            ids = file.readlines()
            lista_ids_solicitantes = [eval(i.strip()) for i in ids]

        ids_lista_solicitantes = {i['id_solicitud'] for i in lista_ids_solicitantes}
        lista_solicitantes = [i for i in lista_solicitantes if i['id_solicitud'] not in ids_lista_solicitantes]

        for i in lista_solicitantes:
            for e in lista_vehiculos:
                if (i["longitude_destino"] == e["longitude_destino"]) and (i["latitude_destino"] == e["latitude_destino"]):
                    if ((i["longitude"] - e["longitude"]) < 0.003) and ((i["latitude"] - e["latitude"]) < 0.003):
                        print(f'El usuario: {i["id_persona"]} ha hecho match con el coche: {e["id_coche"]}')
                        print(i)
                        records = {'id_solicitante': i["id_solicitud"], 'id_vehiculo': e["id_oferta"], 'latitud_solicitante': i['latitude'],
                                   'longitud_solicitante': i['longitude'], 'latitud_vehiculo': i['latitude'],
                                   'longitud_vehiculo': i['longitude'], 'latitud_final_solicitante': i['latitude_destino'],
                                   'longitud_final_solicitante': i['longitude_destino'], 'latitud_final_vehiculo': i['latitude_destino'],
                                   'longitud_final_vehciulo': i['longitude_destino'], 'match': 'yes'}
                        lista_matches.append(records)
                        for i in lista_matches:
                            lista_ids_solicitantes.append(i['id_solcitante'])
                        
                    else:
                        print(f'El usuario: {i["id_persona"]} NO! ha hecho match con el coche: {e["id_coche"]}')
                        print(i, e)
                        records = {'id_solicitante': i["id_solicitud"], 'id_vehiculo': e["id_oferta"], 'latitud_solicitante': i['latitude'],
                                   'longitud_solicitante': i['longitude'], 'latitud_vehiculo': e['latitude'],
                                   'longitud_vehiculo': e['longitude'], 'latitud_final_solicitante': i['latitude_destino'],
                                   'longitud_final_solicitante': i['longitude_destino'], 'latitud_final_vehiculo': e['latitude_destino'],
                                   'longitud_final_vehciulo': e['longitude_destino'], 'match': 'no'}
                        lista_no_matches.append(records)
                else:
                    print(f'El usuario: {i["id_persona"]} NO! ha hecho match con el coche: {e["id_coche"]}')
                    print(i, e)
                    records = {'id_solicitante': i["id_solicitud"], 'id_vehiculo': e["id_oferta"], 'latitud_solicitante': i['latitude'],
                                   'longitud_solicitante': i['longitude'], 'latitud_vehiculo': e['latitude'],
                                   'longitud_vehiculo': e['longitude'], 'latitud_final_solicitante': i['latitude_destino'],
                                   'longitud_final_solicitante': i['longitude_destino'], 'latitud_final_vehiculo': e['latitude_destino'],
                                   'longitud_final_vehciulo': e['longitude_destino'], 'match': 'no'}
                    lista_no_matches.append(records)

        with open('ids_solicitantes.txt', 'w') as file:
            for id_solicitante in lista_ids_solicitantes:
                file.write(id_solicitante + '\n')

        yield lista_no_matches

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        project=project_id,
        runner="DirectRunner",
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west6"
    )) as p:
        p1 = (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_ofertas}')
            | "Decode msg" >> beam.Map(decode_message)
            | "Window1" >> beam.WindowInto(beam.window.FixedWindows(2))
        )

        p2 = (
            p 
            | "ReadFromPubSub2" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_solicitudes}')
            | "Decode msg2" >> beam.Map(decode_message)
            | "Window2" >> beam.WindowInto(beam.window.FixedWindows(2))
        )

        data = (p1, p2)
        merged = (
            data 
            | "Merge PCollections" >> beam.Flatten()
            | "Find Matches" >> beam.ParDo(OutputDoFn())
            #| "Write to BigQuery" >> beam.io.WriteToBigQuery(
            #    table=f"{project_id}:{bq_dataset}.{bq_table}",
            #    schema="id_solicitante:STRING, id_vehiculo:STRING, latitud_solicitante:STRING, longitud_solicitante:STRING, latitud_vehiculo:STRING, longitud_vehiculo:STRING, latitud_final_solicitante:STRING, longitud_final_solicitante:STRING, latitud_final_vehiculo:STRING, longitud_final_vehciulo:STRING",
            #    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            #    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            #)
            | "Find Matches2" >> beam.Map(print)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")
    run()
