*INSTRUCCIONES PARA LA EJECUCIÓN DEL CÓDIGO*

1. Abris la carpeta ejecutable en vuestro Visual Studio Code
2. Se instala el requirements.txt
3. Ejecutais el api_glcoud.py y os secuestrara la consola
4. Se ejecuta el archivo subir_archivos_kml_al_bucket (solo hace falta una vez, o cada vez que añadamos archivos .kml)
5. Ejecutais el generador.py con el siguiente comando: "python generador.py --project_id nombredelproyecto --topic_name nombredeltopic --bucket_name nombredelbucket"
6. Vereis los mensajes en terminal y en el bucket de Pub/Sub conforme se generen.
7. Para el dataflow, ejecutar el siguiente codigo 
python dataflow.py `
    --project_id "nombredelproyecto" `
	--topic_name "nombredeltopic" `
    --input_subscription <YOUR_INPUT_PUBSUB_SUBSCRIPTION_NAME> `
    --output_topic <YOUR_OUTPUT_PUBSUB_TOPIC_NAME> `
    --runner "DataflowRunner" `
    --job_name <YOUR_DATAFLOW_JOB> `
    --region <YOUR_REGION_ID> `