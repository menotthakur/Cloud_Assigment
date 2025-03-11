import json
import boto3
import os
import pandas as pd
import io
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

endpoint_url = 'http://localhost:4566'
s3_client = boto3.client('s3', endpoint_url=endpoint_url)
dynamodb_client = boto3.resource('dynamodb', endpoint_url=endpoint_url)

MAX_FILE_SIZE = 10 * 1024 * 1024

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Processing file: {key} from bucket: {bucket}")
        
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']
        
        if file_size > MAX_FILE_SIZE:
            error_msg = f"File size {file_size} bytes exceeds maximum allowed size of {MAX_FILE_SIZE} bytes"
            logger.error(error_msg)
            return {'statusCode': 400, 'body': json.dumps({'error': error_msg})}
        
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = obj['Body'].read()
        
        try:
            df = pd.read_csv(io.BytesIO(file_content))
        except Exception as e:
            error_msg = f"Error parsing CSV file: {str(e)}"
            logger.error(error_msg)
            return {'statusCode': 400, 'body': json.dumps({'error': error_msg})}
        
        metadata = {
            'filename': key,
            'upload_timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_size_bytes': file_size,
            'row_count': len(df),
            'column_count': len(df.columns),
            'column_names': df.columns.tolist()
        }
        
        logger.info(f"Extracted metadata: {metadata}")
        
        table = dynamodb_client.Table('csv_metadata')
        table.put_item(Item=metadata)
        
        logger.info(f"Metadata for {key} successfully stored in DynamoDB")
        
        return {'statusCode': 200, 'body': json.dumps({'message': 'CSV file processed successfully', 'metadata': metadata})}
        
    except Exception as e:
        error_message = f"Error processing CSV file: {str(e)}"
        logger.error(error_message)
        return {'statusCode': 500, 'body': json.dumps({'error': error_message})}