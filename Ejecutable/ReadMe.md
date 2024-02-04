Os dejo las intrucciones para ejecutar el codigo

1. Abris la carpeta ejecutable en vuestro Visual Studio Code
2. Es posible que tengais que instalar alguna libreria (haremos un requirements.txt y ya no habra problema)
3. Instalais las librerias necesarias
4. Ejecutais el crear_api.py y os secuestrara la consola
5. Desde vuestros archivos (fuera de Visual), haceis doble click sobre el subir_coords.html, y se os abrirá una pestaña en el buscador de internet
6. Ahi, le dais al boton de seleccionar archivos y subis todos los archivos kml de golpe y le dais a Submit, y os aparecera un mensaje de se han subido correctamente
7. Os volveis al visual y ahi, podeis cambiar si quereis cuantas solicitudes se hacen (ahora estan en 3), y ejecutais el main.py con el siguiente comando: "python main.py --project_id nombredelproyecto --topic_name nombredeltopic"
8. Vereis los mensajes en terminal y en el bucket de Pub/Sub conforme se generen.
9. Para el dataflow corri el siguiente codigo 
python dataflow.py `
    --project_id "nombredelproyecto" `
	--topic_name "nombredeltopic" `
    --input_subscription <YOUR_INPUT_PUBSUB_SUBSCRIPTION_NAME> `
    --output_topic <YOUR_OUTPUT_PUBSUB_TOPIC_NAME> `
    --runner "DataflowRunner" `
    --job_name <YOUR_DATAFLOW_JOB> `
    --region <YOUR_REGION_ID> `