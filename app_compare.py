import tempfile
import os
import json
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

from document_compare import compare_documents

app = Flask(__name__)

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'pdf'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/compare', methods=['POST'])
def compare_documents_endpoint():
    """Compare two uploaded documents"""
    try:
        # Check if both files are present
        if 'document1' not in request.files or 'document2' not in request.files:
            return jsonify({
                'error': 'Both document1 and document2 files are required'
            }), 400
        
        file1 = request.files['document1']
        file2 = request.files['document2']
        
        # Check if files are selected
        if file1.filename == '' or file2.filename == '':
            return jsonify({
                'error': 'No files selected'
            }), 400
        
        # Check file types
        if not (allowed_file(file1.filename) and allowed_file(file2.filename)):
            return jsonify({
                'error': f'Allowed file types: {", ".join(ALLOWED_EXTENSIONS)}'
            }), 400
        
        # Save files temporarily
        temp_files = []
        try:
            # Save document 1
            tf1 = tempfile.NamedTemporaryFile(delete=False, suffix='.'+file1.filename.rsplit('.', 1)[1].lower())
            file1.save(tf1.name)
            temp_files.append(tf1.name)
            
            # Save document 2
            tf2 = tempfile.NamedTemporaryFile(delete=False, suffix='.'+file2.filename.rsplit('.', 1)[1].lower())
            file2.save(tf2.name)
            temp_files.append(tf2.name)
            
            # Compare documents
            results = compare_documents(tf1.name, tf2.name)
            
            # Clean up temp files
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass
            
            return jsonify(results)
            
        except Exception as e:
            # Clean up temp files on error
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass
            raise e
            
    except Exception as e:
        return jsonify({
            'error': f'Error processing documents: {str(e)}'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'document-comparison-api'
    })

@app.route('/', methods=['GET'])
def index():
    """API information endpoint"""
    return jsonify({
        'service': 'Document Comparison API',
        'version': '1.0.0',
        'endpoints': {
            '/compare': {
                'method': 'POST',
                'description': 'Compare two documents',
                'parameters': {
                    'document1': 'File upload (required)',
                    'document2': 'File upload (required)'
                },
                'supported_formats': list(ALLOWED_EXTENSIONS)
            },
            '/health': {
                'method': 'GET',
                'description': 'Health check'
            }
        },
        'example_usage': {
            'curl': "curl -X POST -F 'document1=@doc1.pdf' -F 'document2=@doc2.pdf' http://localhost:5000/compare"
        }
    })

@app.errorhandler(413)
def too_large(e):
    return jsonify({
        'error': 'File too large. Maximum size is 16MB.'
    }), 413

if __name__ == '__main__':
    print("🚀 Starting Document Comparison API...")
    print("📄 Endpoints:")
    print("  POST /compare - Compare two documents")
    print("  GET  /health  - Health check")
    print("  GET  /        - API information")
    print("")
    app.run(host='0.0.0.0', port=5000, debug=False)