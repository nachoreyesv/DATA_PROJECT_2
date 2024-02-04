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

class PubSubMessages:

    def _init_(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publish_message(self, data):
        json_str = json.dumps(data)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))

    def exit(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")


class ReadKmlDoFn(beam.DoFn):
    def process(self, element):
        json_data = json.loads(element)
        oferta = json_data.get("id_oferta")
        kml_file = json_data.get("kml_file")
        project_id = json_data.get("project_id")
        topic_name = json_data.get("topic_name")

        if oferta and kml_file and project_id and topic_name:
            pubsub_class = PubSubMessages(project_id, topic_name)

            data = {"id_oferta": [], "punto": [], "latitude": [], "longitude": []}

            with open(kml_file, "r", encoding="utf-8") as file:
                kml_data = file.read()
            root = ET.fromstring(kml_data)
            coords = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")
            if coords is not None:
                coords_str = coords.text
                coords_list = [tuple(map(float, _.split(','))) for _ in coords_str.split()]
                for _, coords in enumerate(coords_list):
                    data["id_oferta"] = oferta
                    data["punto"] = _ + 1
                    data["latitude"] = coords[1]
                    data["longitude"] = coords[0]
                    pubsub_class.publish_message(data)
                    time.sleep(5)


def gen_ofertas(num_ofertas, project_id, topic_name):
    return [
        {
            "id_oferta": i,
            "kml_file": obtener_ruta_archivo_aleatorio(),
            "project_id": project_id,
            "topic_name": topic_name
        } for i in range(1, num_ofertas + 1)
    ]


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
            | "Parse JSON messages" >> beam.ParDo(ReadKmlDoFn())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{bq_dataset}.{bq_table}", # Required Format: PROJECT_ID:DATASET.TABLE
                schema='nombre:STRING', # Required Format: field:TYPE
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )    
        )


if _name_ == '_main_':
    # Set Logs
    logging.getLogger().setLevel(logging.INFO)
    logging.info("The process started")

    # Run Process
    run()