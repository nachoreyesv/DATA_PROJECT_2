from flask import Flask, request, jsonify
import os

app = Flask(__name__)

UPLOAD_FOLDER = 'coordenadas/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

COORDENADAS_FOLDER = 'coordenadas'

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return jsonify({'error': 'No files part'})

    files = request.files.getlist('files[]')

    for file in files:
        if file.filename == '':
            return jsonify({'error': 'No selected file'})

        if file:
            filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filename)

    return jsonify({'message': 'Files uploaded successfully'})

@app.route('/get_kml/<int:file_id>', methods=['GET'])
def get_kml(file_id):
    filename = f'{file_id}.kml'
    file_path = os.path.join(COORDENADAS_FOLDER, filename)
    return jsonify({'file_path': file_path})

if __name__ == '__main__':
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    app.run(debug=True)
