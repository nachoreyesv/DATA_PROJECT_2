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

project_id = "dataproject2-413213"
bq_dataset = "tu_dataset"
bq_table = "tu_tabla"

def ParsePubSubMessage(message):

    # Decode PubSub message in order to deal with
    pubsub_message = message.decode('utf-8')
    
    # Convert string decoded in JSON format
    msg = json.loads(pubsub_message)

    logging.info("New message in PubSub: %s", msg)

    # Return function
    return msg


def run():
    # Input arguments
    parser = argparse.ArgumentParser(description=('Dataflow Streaming Pipeline'))

    parser.add_argument(
        '--project_id',
        required=True,
        help='GCP cloud project name.')

    parser.add_argument(
        '--input_subscription',
        required=True,
        help='PubSub subscription from which we will read data from the generator.')

    parser.add_argument(
        '--output_topic',
        required=False,
        help='PubSub Topic which will be the sink for our data.')

    args, pipeline_opts = parser.parse_known_args()

    """ Apache Beam Pipeline """

    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
                              save_main_session=True, streaming=True, project=args.project_id)

    # Pipeline
    with beam.Pipeline(argv=pipeline_opts, options=options) as p:
        """ Part 01: Read data from PubSub. """
        _ = (
            p
            | "Read From PubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "Parse JSON messages" >> beam.Map(ParsePubSubMessage)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{bq_dataset}.{bq_table}", # Required Format: PROJECT_ID:DATASET.TABLE
                schema='nombre:STRING', # Required Format: field:TYPE
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )   
        )


if __name__ == '__main__':
    # Set Logs
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")

    # Run Process
    run()