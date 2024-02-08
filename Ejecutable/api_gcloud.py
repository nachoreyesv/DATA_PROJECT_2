from flask import Flask, request, jsonify
from google.cloud import storage
import sys

app = Flask(__name__)

if len(sys.argv) != 3:
    print("Usage: python3 api_gcloud.py entregablecloudsinmiguel kmlsdp2")
    sys.exit(1)

project_id = sys.argv[1]
BUCKET_NAME = sys.argv[2]


storage_client = storage.Client(project_id)


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
