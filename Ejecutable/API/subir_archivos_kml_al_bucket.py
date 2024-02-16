import os
import requests

api_url = 'https://us-central1-dataflow-clase.cloudfunctions.net/main'
folder_path = 'coordenadas'
files = [('files[]', (file, open(os.path.join(folder_path, file), 'rb'))) for file in os.listdir(folder_path) if file.endswith('.kml')]

response = requests.post(api_url, files=files)

print(response.text)