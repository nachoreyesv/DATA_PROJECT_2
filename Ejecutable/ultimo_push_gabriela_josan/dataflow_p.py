import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
import xml.etree.ElementTree as ET

BASE_URL = 'http://127.0.0.1:5000'
NUM_OFERTAS = 3  # Número de ofertas que deseas generar
DOWNLOAD_FOLDER = 'get_coord'

import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ParsePubSubMessage(beam.DoFn):
    """Clase DoFn para parsear mensajes de Pub/Sub."""

    def process(self, element):
        """Decodifica y parsea los mensajes de Pub/Sub en formato JSON."""
        pubsub_message = element.decode('utf-8')
        msg = json.loads(pubsub_message)
        logging.info("Nuevo mensaje en Pub/Sub: %s", msg)
        yield msg

class MatchRequestsToOffers(beam.DoFn):
    """Clase DoFn para emparejar solicitudes con ofertas basándose en la latitud y longitud."""

    def process(self, element, offers_dict):
        """Empareja cada solicitud con ofertas cercanas basadas en la latitud y longitud."""
        request = element
        for offer in offers_dict:
            # Verificar si la solicitud y la oferta están lo suficientemente cerca (aquí puede colocar su lógica de distancia)
            if abs(request['latitude'] - offer['latitude']) < 0.1 and abs(request['longitude'] - offer['longitude']) < 0.1:
                yield {'request': request, 'offer': offer}

def run():
    # Argumentos de entrada
    parser = argparse.ArgumentParser(description=('Dataflow Streaming Pipeline'))
    parser.add_argument(
        '--project_id',
        required=True,
        help='Nombre del proyecto en GCP.')
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='Suscripción de Pub/Sub de la cual leeremos datos del generador.')
    args, pipeline_opts = parser.parse_known_args()

    # Opciones del pipeline
    pipeline_options = PipelineOptions(pipeline_opts, streaming=True, project=args.project_id)

    with beam.Pipeline(options=pipeline_options) as p:
        # Leer solicitudes y ofertas desde Pub/Sub
        requests = p | "Leer solicitudes desde Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
        offers = p | "Leer ofertas desde Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.input_offer_subscription)

        # Parsear los mensajes JSON
        parsed_requests = requests | "Parsear solicitudes JSON" >> beam.ParDo(ParsePubSubMessage())
        parsed_offers = offers | "Parsear ofertas JSON" >> beam.ParDo(ParsePubSubMessage())

        # Convertir ofertas a un diccionario para su posterior procesamiento eficiente
        offers_dict = parsed_offers | "Convertir ofertas a diccionario" >> beam.CombineGlobally(beam.combiners.ToList()).without_defaults()

        # Emparejar solicitudes con ofertas
        matched_requests = (
            parsed_requests
            | "Emparejar solicitudes con ofertas" >> beam.ParDo(MatchRequestsToOffers(), beam.pvalue.AsList(offers_dict))
        )

        # Escribir los resultados en BigQuery
        matched_requests | "Escribir resultados en BigQuery" >> beam.io.WriteToBigQuery(
            table=f"{args.project_id}:{bq_dataset}.{bq_table}",
            schema='request:STRING, offer:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run()
