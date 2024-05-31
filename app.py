import tempfile
import os
from flask import Flask, request, jsonify
from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.cosmos import CosmosClient
import uuid
from dotenv import load_dotenv

load_dotenv('.env')
app = Flask(__name__)

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')

# Initialize Azure Service Bus client
service_bus_client = ServiceBusClient.from_connection_string(os.getenv('AZURE_SERVICE_BUS_CONNECTION_STRING'))
queue_name = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME')

# Initialize Azure Cosmos DB client
cosmos_client = CosmosClient(os.getenv('AZURE_COSMOS_DB_ENDPOINT'), os.getenv('AZURE_COSMOS_DB_KEY'))
database_name = os.getenv('AZURE_COSMOS_DB_DATABASE_NAME')
cosmos_container_name = os.getenv('AZURE_COSMOS_DB_CONTAINER_NAME')

# Define Cosmos DB container
cosmos_db = cosmos_client.get_database_client(database_name)
cosmos_container = cosmos_db.get_container_client(cosmos_container_name)

@app.route('/', methods=['POST'])
def upload_image():
    file = request.files['image']
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(file.stream.read())
    temp_file.close()

    # Generate a unique name for the blob
    blob_name = str(uuid.uuid4()) + "_" + file.filename

    # Upload image to Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(temp_file.name, "rb") as data:
        blob_client.upload_blob(data)

    # Publish event to Service Bus
    blob_url = f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net/{container_name}/{blob_name}"
    message = ServiceBusMessage(blob_url)
    sender = service_bus_client.get_queue_sender(queue_name=queue_name)
    sender.send_messages(message)

    return jsonify({'status': 'success', 'blob_url': blob_url})

@app.route('/status', methods=['GET'])
def check_status():
    blob_url = request.args.get('blob_url')
    query = f"SELECT * FROM c WHERE c.blob_url='{blob_url}'"
    items = list(cosmos_container.query_items(query=query, enable_cross_partition_query=True))
    if items:
        return jsonify(items[0])
    else:
        return jsonify({'status': 'processing'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
