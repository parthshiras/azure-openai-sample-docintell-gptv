import os
import logging
import tempfile
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from gptv import get_doc_int_results, analyze_with_gpt
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosHttpResponseError
import random

load_dotenv('.env')

# Create local vars from the environment variables
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
    
    # Initialize retry counts
    application_properties = msg.application_properties or {}
    total_retry_count = application_properties.get('total_retry_count', 0)
    doc_int_results_id = application_properties.get('doc_int_results_id', None)

    docint_call_retry_count = 0
    gptv_call_retry_count = 0
    
    doc_int_results = None
    doc_int_barcode = None
    
    # Extract blob name from the blob URL
    blob_name = blob_url.split('/')[-1]

    blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_name)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(blob_client.download_blob().readall())
        temp_file.close()

    # First API Call: Get document intelligence results if not already available
    if not doc_int_results_id:
        while docint_call_retry_count < max_retries:
            try:
                logging.info('Getting document intelligence results...')
                doc_int_api_results = get_doc_int_results(temp_file.name)
                logging.info(f'Document intelligence results: {doc_int_api_results}')
                doc_int_results = doc_int_api_results.content
                doc_int_barcode = doc_int_api_results.pages[0].barcodes[0].value if doc_int_api_results.pages[0].barcodes else "n/a"
                break
            except Exception as e:
                docint_call_retry_count += 1
                delay = (2 ** docint_call_retry_count)
                delay += random.uniform(0, 10)  # Add random delay to avoid retrying at the same time
                logging.error(f"Error getting document intelligence results: {e}. Retrying {docint_call_retry_count}/{max_retries} after {delay} seconds.")
                time.sleep(delay)


        if doc_int_results is None:
            total_retry_count += 1
            # If total retries exceed max_retries, send to dead-letter queue
            if total_retry_count >= max_retries:
                logging.error(f'Max retries reached for {blob_url}. Sending to dead-letter queue.')
                send_to_dead_letter_queue(blob_url, total_retry_count, doc_int_results_id)
                return
            put_message_back_on_queue(blob_url, total_retry_count, doc_int_results_id)
            return
    else:
        # Retrieve document intelligence results from Cosmos DB
        try:
            doc_int_data = container.read_item(item=doc_int_results_id, partition_key=doc_int_results_id)
            doc_int_results = doc_int_data['results']
            doc_int_barcode = doc_int_data['barcode']
        except CosmosHttpResponseError as e:
            logging.error(f"Error retrieving document intelligence results from Cosmos DB: {e}")
            put_message_back_on_queue(blob_url, total_retry_count, doc_int_results_id)
            return
    
    # gptv API Call: Analyze with GPT
    gptv_api_results = None
    while gptv_call_retry_count < max_retries:
        try:
            logging.info('Analyzing image with GPT...')
            gptv_api_results = analyze_with_gpt(temp_file.name, doc_int_results, doc_int_barcode)
            logging.info(f'GPT analysis complete: {gptv_api_results}')
            break
        except Exception as e:
            gptv_call_retry_count += 1
            delay = 2 ** gptv_call_retry_count  # Exponential backoff
            delay += random.uniform(0, 10)  # Add random delay to avoid retrying at the same time
            logging.error(f"Error analyzing image with GPT: {e}. Retrying {gptv_call_retry_count}/{max_retries} after {delay} seconds.")
            time.sleep(delay)

    if gptv_api_results is None:
        total_retry_count += 1
        # If total retries exceed max_retries, send to dead-letter queue
        if total_retry_count >= max_retries:
            logging.error(f'Max retries reached for {blob_url}. Sending to dead-letter queue.')
            send_to_dead_letter_queue(blob_url, total_retry_count, doc_int_results_id)
            return
        # Put message back on the queue
        logging.info('*=*=*=*=*=*=*=*=*=* Putting message back on queue in gptv2 *=*=*=*=*=*=*=*=*=*')
        doc_int_results_id = store_doc_int_results(blob_name, doc_int_results, doc_int_barcode)
        put_message_back_on_queue(blob_url, total_retry_count, doc_int_results_id)
        return

    # Convert Pydantic model to dictionary and add blob_url and id
    metadata_dict = gptv_api_results.model_dump()
    metadata_dict['id'] = blob_name  # Use the blob name as the unique ID
    metadata_dict['blob_url'] = blob_url

    try:
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
        doc_int_results_id = store_doc_int_results(blob_name, doc_int_results, doc_int_barcode)
        put_message_back_on_queue(blob_url, total_retry_count, doc_int_results_id)

def store_doc_int_results(blob_name, doc_int_results, doc_int_barcode):
    """Store the document intelligence results in Cosmos DB for future retries."""
    if doc_int_results:
        doc_int_results_id = f"doc_int_results_{blob_name}"
        doc_int_data = {
            'id': doc_int_results_id,
            'results': doc_int_results,
            'barcode': doc_int_barcode
        }
        try:
            container.create_item(body=doc_int_data)
            logging.info(f'Document intelligence results stored in Cosmos DB with id: {doc_int_results_id}')
            return doc_int_results_id
        except CosmosResourceExistsError:
            logging.error(f"Document intelligence results already exist with id: {doc_int_results_id}")
        except CosmosHttpResponseError as e:
            logging.error(f"Error storing document intelligence results: {e}")
            return doc_int_results_id  # Return id even if there's an error

def put_message_back_on_queue(blob_url,total_retry_count, doc_int_results_id):
    new_msg = ServiceBusMessage(
        blob_url,
        application_properties={
            'total_retry_count': total_retry_count,
            'doc_int_results_id': doc_int_results_id
        }
    )
    sender = service_bus_client.get_queue_sender(queue_name=queue_name)
    sender.send_messages(new_msg)
    logging.info(f'Message with blob URL {blob_url} put back on queue for retry.')

def send_to_dead_letter_queue(blob_url, total_retry_count, doc_int_results_id):
    dead_letter_sender = service_bus_client.get_queue_sender(queue_name=f"{queue_name}/$deadletterqueue")
    dead_letter_sender.send_messages(ServiceBusMessage(
        blob_url,
        application_properties={
            'total_retry_count': total_retry_count,
            'doc_int_results_id': doc_int_results_id
        }
    ))
    logging.error(f'Message with blob URL {blob_url} sent to dead-letter queue after {max_retries} retries.')
