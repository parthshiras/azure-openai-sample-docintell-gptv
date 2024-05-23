import logging
import sys
import tempfile
import openai
import requests
import subprocess  # This import might not be necessary
from flask import Flask
from flask import request
import flask.json as flask_json

from gptv import process_image


app = Flask(__name__)

@app.route('/', methods=['POST'])
def get_jpg_and_execute():
    
    try:
       file = request.files['image']
       tf = tempfile.NamedTemporaryFile(delete=False)
       tf.write(file.stream.read())
       tf.close()
       
       oimg1 = process_image(tf.name)
       json = oimg1.model_dump_json()
       json = json.replace('"n/a"', 'null')
    except openai.RateLimitError as e:
       logging.error(f"Rate limit error: {e}")
       return app.response_class(
              response=f'Rate limit error {e.message}',
              status=e.code
       )
    except requests.exceptions.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise e
    
    response = app.response_class(
        response=json,
        mimetype='application/json'
    )
    return response

if __name__ == '__main__':
  logging.basicConfig(stream=sys.stderr, level=logging.WARN)
  app.run(host='0.0.0.0', port='5000', debug=False)  # Disable debug mode for production


