import tempfile
import requests
import subprocess  # This import might not be necessary
from flask import Flask
from flask import request

from gptv import process_image


app = Flask(__name__)

@app.route('/', methods=['POST'])
def get_static_info_and_execute():
    try:
        # Get the image URL from the POST request data
        data = request.get_json()
        image_url = data['url']  # Replace 'url' with the actual key in the JSON data

        # Download the image and save it to a temporary file
        response = requests.get(image_url)
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write(response.content)
        tf.close()

        # Pass the temporary file path to the process_image function
        oimg1 = process_image(tf.name)
        static_string = f"Processed image data: {oimg1}"
    except Exception as e:
        static_string = f"Error processing image: {e}"
    return static_string


if __name__ == '__main__':
  app.run(host='0.0.0.0', port='5000', debug=False)  # Disable debug mode for production


