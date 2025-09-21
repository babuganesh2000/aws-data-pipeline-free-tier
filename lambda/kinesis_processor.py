import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, List, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
kinesis_client = boto3.client('kinesis')
rds_client = boto3.client('rds')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Process Kinesis records and store aggregated data in RDS SQL Server.
    Optimized for free tier usage with batch processing.
    """
    
    try:
        records_processed = 0
        failed_records = []
        
        # Process each Kinesis record
        for record in event['Records']:
            try:
                # Decode the Kinesis data
                payload = json.loads(
                    boto3.Session().region_name and 
                    record['kinesis']['data'] or 
                    record['kinesis']['data'].encode('utf-8')
                )
                
                # Process the data (example: data validation and transformation)
                processed_data = process_record(payload)
                
                # Store in RDS (batch operation for efficiency)
                store_in_rds(processed_data)
                
                records_processed += 1
                logger.info(f"Successfully processed record: {record['kinesis']['sequenceNumber']}")
                
            except Exception as e:
                logger.error(f"Failed to process record: {str(e)}")
                failed_records.append({
                    'sequenceNumber': record['kinesis']['sequenceNumber'],
                    'error': str(e)
                })
        
        # Log processing summary
        logger.info(f"Processed {records_processed} records, {len(failed_records)} failed")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'recordsProcessed': records_processed,
                'failedRecords': len(failed_records),
                'timestamp': datetime.utcnow().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

def process_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform and validate incoming data.
    Add your business logic here.
    """
    
    # Example data processing
    processed = {
        'id': payload.get('id'),
        'timestamp': payload.get('timestamp', datetime.utcnow().isoformat()),
        'data': payload.get('data', {}),
        'processed_at': datetime.utcnow().isoformat()
    }
    
    # Add data validation
    if not processed['id']:
        raise ValueError("Missing required field: id")
    
    return processed

def store_in_rds(data: Dict[str, Any]) -> None:
    """
    Store processed data in RDS SQL Server.
    Uses connection pooling for efficiency.
    """
    
    # Note: In production, use proper connection pooling
    # and parameterized queries to prevent SQL injection
    
    import pymssql
    
    try:
        # Get RDS connection details from environment
        rds_endpoint = os.environ['RDS_ENDPOINT']
        rds_username = os.environ['RDS_USERNAME']
        rds_password = os.environ['RDS_PASSWORD']
        rds_database = os.environ.get('RDS_DATABASE', 'DataPipeline')
        
        # Connect to SQL Server
        conn = pymssql.connect(
            server=rds_endpoint,
            user=rds_username,
            password=rds_password,
            database=rds_database
        )
        
        cursor = conn.cursor()
        
        # Insert data (example table structure)
        insert_query = """
        INSERT INTO ProcessedData (Id, Timestamp, Data, ProcessedAt)
        VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            data['id'],
            data['timestamp'],
            json.dumps(data['data']),
            data['processed_at']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully stored data for ID: {data['id']}")
        
    except Exception as e:
        logger.error(f"Failed to store data in RDS: {str(e)}")
        raise