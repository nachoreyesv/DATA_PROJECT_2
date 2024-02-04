from flask import Flask, request, jsonify
from google.cloud import storage

app = Flask(__name__)

storage_client = storage.Client('dataflow-clase')

BUCKET_NAME = 'data-flow-bucket-dp2'

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
    app.run(debug=True)
