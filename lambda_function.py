import json
from csv_processor import CSVProcessor
import logging


def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    try:
        data_processor = CSVProcessor()
        bucket_name, file_key = event['Records'][0]['s3']['bucket']['name'], event['Records'][0]['s3']['object']['key']
        logging.info("Bucket Name::", bucket_name)
        logging.info("File Key::", file_key)
        aggregated_totals = data_processor.process_csv(bucket_name, file_key)
        boolT = data_processor.write_to_dynamodb(aggregated_totals)
        logging.info(boolT)
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'CSV data processed successfully.'})
        }
    except Exception as e:
        print("Error processing CSV:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error.'})
        }

       