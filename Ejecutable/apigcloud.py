from flask import Flask, request, jsonify
from google.cloud import storage

app = Flask(__name__)

# Definir el cliente de Cloud Storage
storage_client = storage.Client()

# Nombre del bucket en Google Cloud Storage
BUCKET_NAME = 'nombre_del_bucket'

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return jsonify({'error': 'No files part'})

    files = request.files.getlist('files[]')

    for file in files:
        if file.filename == '':
            return jsonify({'error': 'No selected file'})

        if file:
            # Obtener el nombre del archivo
            filename = file.filename

            # Obtener el bucket
            bucket = storage_client.bucket(BUCKET_NAME)

            # Crear un blob en el bucket con el nombre del archivo
            blob = bucket.blob(filename)

            # Subir el archivo al blob
            blob.upload_from_string(file.read())

    return jsonify({'message': 'Files uploaded successfully'})

@app.route('/get_kml/<int:file_id>', methods=['GET'])
def get_kml(file_id):
    filename = f'{file_id}.kml'
    # Generar la URL del archivo en el bucket
    file_url = f'https://storage.googleapis.com/{BUCKET_NAME}/{filename}'
    return jsonify({'file_url': file_url})

if __name__ == '__main__':
    app.run(debug=True)
