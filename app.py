import tempfile
import requests
import random
import string
from flask import Flask, request
import flask.json as flask_json
import logging

from gptv import process_image

app = Flask(__name__)

TEST = False

def get_random_string(length=8):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

def get_random_price():
    return f"{random.uniform(1, 100):.2f}"

def get_dummy_response():
    dummy_data = {
        "brand": get_random_string(),
        "product_name": get_random_string(),
        "price": get_random_price(),
        "price_per_unit": get_random_price(),
        "expiration_date": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        "article_number": ''.join(random.choices(string.digits, k=12)),
        "bar_code_available": random.choice([True, False]),
        "bar_code_numbers": ''.join(random.choices(string.digits, k=13)) if random.choice([True, False]) else "n/a"
    }
    return flask_json.dumps(dummy_data)

@app.route('/', methods=['POST'])
def get_jpg_and_execute():                  
    file = request.files['image']
    tf = tempfile.NamedTemporaryFile(delete=False)
    tf.write(file.stream.read())
    tf.close()
    
    try:
        # Intentionally raise an exception to always generate a dummy response
        if TEST:
            raise requests.exceptions.RequestException("Forced exception for testing")
        
        oimg1 = process_image(tf.name)
        json_response = oimg1.model_dump_json()
        json_response = json_response.replace('"n/a"', 'null')
    except requests.exceptions.RequestException as e:
        logging.error(f"API call failed: {e}")
        if TEST:
            json_response = get_dummy_response()
        else:
            raise e
    
    response = app.response_class(
        response=json_response,
        mimetype='application/json'
    )
    return response

"""
#Code below can be used as replacement if the image is posted via URL instead of a binary
def get_jpg_and_execute():
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
"""

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5000', debug=False)
