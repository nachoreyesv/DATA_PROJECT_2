from flask import Flask, request, jsonify
from google.cloud import storage
from functions_framework import create_app
import os
import sys

app = Flask(__name__)

BUCKET_NAME = 'data-flow-bucket-dp2'

storage_client = storage.Client()

def main(request):
    # Verifica el método de la solicitud
    if request.method == 'POST':
        # Procesa la carga de archivos
        files = request.files.getlist('files[]')
        for file in files:
            if file.filename == '':
                return jsonify({'error': 'No selected file'})
            if file:
                filename = file.filename
                bucket = storage_client.bucket(BUCKET_NAME)
                blob = bucket.blob(filename)
                blob.upload_from_string(file.read())

        return jsonify({'message': 'Files uploaded successfully'})
    elif request.method == 'GET':
        # Procesa la solicitud GET para obtener KML
        file_id = request.args.get('file_id')
        if file_id is not None:
            filename = f'{file_id}.kml'
            file_url = f'https://storage.googleapis.com/{BUCKET_NAME}/{filename}'
            return jsonify({'file_url': file_url})
        else:
            return jsonify({'error': 'Missing file_id parameter'})
    else:
        return jsonify({'error': 'Method not allowed'})

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return jsonify({'error': 'No files part'})

    files = request.files.getlist('files[]')

    for file in files:
        if file.filename == '':
            return jsonify({'error': 'No selected file'})

        if file:
            filename = file.filename
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(filename)
            blob.upload_from_string(file.read())

    return jsonify({'message': 'Files uploaded successfully'})

@app.route('/get_kml/<int:file_id>', methods=['GET'])
def get_kml(file_id):
    filename = f'{file_id}.kml'
    file_url = f'https://storage.googleapis.com/{BUCKET_NAME}/{filename}'
    return jsonify({'file_url': file_url})

if __name__ == '__main__':
    if 'FUNCTION_TARGET' in os.environ:
        app = create_app()
    else:
        app.run(debug=True)