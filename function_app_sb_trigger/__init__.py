import os
import logging
import tempfile
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from gptv import process_image
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosHttpResponseError

load_dotenv('.env')

# create locl vars from the environment variables
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_BLOB_CONTAINER_NAME = os.getenv('AZURE_BLOB_CONTAINER_NAME')
AZURE_SERVICE_BUS_CONNECTION_STRING = os.getenv('AZURE_SERVICE_BUS_CONNECTION_STRING')
AZURE_SERVICE_BUS_QUEUE_NAME = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME')
AZURE_COSMOS_DB_ENDPOINT = os.getenv('AZURE_COSMOS_DB_ENDPOINT')
AZURE_COSMOS_DB_KEY = os.getenv('AZURE_COSMOS_DB_KEY')
AZURE_COSMOS_DB_DATABASE_NAME = os.getenv('AZURE_COSMOS_DB_DATABASE_NAME')
AZURE_COSMOS_DB_CONTAINER_NAME = os.getenv('AZURE_COSMOS_DB_CONTAINER_NAME')


# Initialize clients
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
blob_container_name = AZURE_BLOB_CONTAINER_NAME
cosmos_client = CosmosClient(AZURE_COSMOS_DB_ENDPOINT, AZURE_COSMOS_DB_KEY)
database_name = AZURE_COSMOS_DB_DATABASE_NAME
container_name = AZURE_COSMOS_DB_CONTAINER_NAME
cosmos_db = cosmos_client.get_database_client(database_name)
container = cosmos_db.get_container_client(container_name)
service_bus_client = ServiceBusClient.from_connection_string(AZURE_SERVICE_BUS_CONNECTION_STRING)
queue_name = AZURE_SERVICE_BUS_QUEUE_NAME
max_retries = 5  # Maximum number of retries before sending to dead-letter queue

def main(msg: func.ServiceBusMessage):
    blob_url = msg.get_body().decode('utf-8')
    logging.info(f'Processing blob URL: {blob_url}')
    
    # Initialize retry count
    application_properties = msg.application_properties or {}
    retry_count = application_properties.get('retry_count', 0)
    
    # Extract blob name from the blob URL
    blob_name = blob_url.split('/')[-1]

    blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_name)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(blob_client.download_blob().readall())
        temp_file.close()
    
    try:
        logging.info('Starting image processing...')
        metadata = process_image(temp_file.name)
        logging.info(f'Image processing complete: {metadata}')
        metadata_dict = metadata.model_dump()
        metadata_dict['id'] = blob_name  # Use the blob name as the unique ID
        metadata_dict['blob_url'] = blob_url

        # Check if the document already exists
        try:
            existing_document = container.read_item(item=blob_name, partition_key=blob_name)
            # If the document exists, merge the new results with the existing document
            for key, value in metadata_dict.items():
                existing_document[key] = value
            container.replace_item(item=existing_document['id'], body=existing_document)
            logging.info('Document updated in Cosmos DB.')
        except CosmosResourceExistsError:
            # If the document does not exist, create a new one
            container.create_item(body=metadata_dict)
            logging.info('New document created in Cosmos DB.')
        except CosmosHttpResponseError as e:
            if e.status_code == 404:
                # If the document does not exist, create a new one
                container.create_item(body=metadata_dict)
                logging.info('New document created in Cosmos DB.')
            else:
                raise

    except Exception as e:
        logging.error(f"Error processing image: {e}", exc_info=True)

        if retry_count < max_retries:
            # Increment retry count and send message back to queue with delay
            retry_count += 1
            delay = 2 ** retry_count  # Exponential backoff
            logging.info(f'Retrying {retry_count}/{max_retries} after {delay} seconds...')
            time.sleep(delay)
            new_msg = ServiceBusMessage(
                blob_url,
                application_properties={'retry_count': retry_count}
            )
            sender = service_bus_client.get_queue_sender(queue_name=queue_name)
            sender.send_messages(new_msg)
        else:
            # Send message to dead-letter queue after max retries
            logging.error('Max retries reached. Sending message to dead-letter queue.')
            dead_letter_sender = service_bus_client.get_queue_sender(queue_name=f"{queue_name}/$deadletterqueue")
            dead_letter_sender.send_messages(ServiceBusMessage(blob_url))


