import os
import requests

api_url = 'http://127.0.0.1:5000/upload'
folder_path = 'coordenadas'
files = [('files[]', (file, open(os.path.join(folder_path, file), 'rb'))) for file in os.listdir(folder_path) if file.endswith('.kml')]

response = requests.post(api_url, files=files)

print(response.json())

