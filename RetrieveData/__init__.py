# RetrieveData/__init__.py
import logging
import json
import os
from azure.data.tables import TableClient
from azure.functions import HttpRequest, HttpResponse


TABLE_CONNECTION_NAME = "AzureWebJobsStorage"
TABLE_NAME = "processeditems"

def main(req: HttpRequest) -> HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    blob_name_query = req.params.get('blobName')
    
    if not blob_name_query:
        return HttpResponse(
             "Please pass a 'blobName' in the query string to retrieve data.",
             status_code=400
        )

    # Connect to the Azure Table Storage
    try:
        table_client = TableClient.from_connection_string(
            conn_str=os.environ[TABLE_CONNECTION_NAME], 
            table_name=TABLE_NAME
        )
    except Exception as e:
        logging.error(f"Error connecting to Table Storage: {e}")
        return HttpResponse("Error connecting to database.", status_code=500)

    query_filter = f"PartitionKey eq '{blob_name_query}'"
    
    entities = table_client.query_entities(query_filter)
    
    result_list = []
    for entity in entities:
        result_list.append(dict(entity))

    # Return the results as a clean JSON response
    return HttpResponse(
        body=json.dumps(result_list),
        mimetype="application/json",
        status_code=200
    )