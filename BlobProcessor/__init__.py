# BlobProcessor/__init__.py
import logging
import pandas as pd
import os
from azure.data.tables import TableClient
from azure.functions import InputStream 

TABLE_CONNECTION_NAME = "AzureWebJobsStorage"
TABLE_NAME = "processeditems"

def main(inputblob: InputStream): 

    name = os.path.basename(inputblob.name) 
    
    logging.info(f"Processing started for blob Name: {name}, Size: {inputblob.length} bytes")
    
    # Read the Blob data using Pandas
    try:
        df = pd.read_csv(inputblob)
    except Exception as e:
        logging.error(f"Error reading CSV for blob {name}: {e}")
        return

    # Connect to the Azure Table Storage securely
    try:
        table_client = TableClient.from_connection_string(
            conn_str=os.environ[TABLE_CONNECTION_NAME], 
            table_name=TABLE_NAME
        )
    except Exception as e:
        logging.error(f"Error connecting to Table Storage: {e}")
        return

    # Iterate over the DataFrame and write entities
    entities_written = 0
    for index, row in df.iterrows():
        
        entity = {
            "PartitionKey": name, 
            "RowKey": str(index), 
            "DataValue": row.iloc[1], 
            "DataID": row.iloc[0] 
        }
        
        table_client.upsert_entity(entity=entity)
        entities_written += 1
        
    logging.info(f"Successfully wrote {entities_written} entities to {TABLE_NAME} for blob {name}.")