import datetime
import uuid
import logging
import boto3
import csv
import io

class CSVProcessor:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.dynamodb_client = boto3.client('dynamodb')
        self.dbresource = boto3.resource('dynamodb')
        self.elist = ['ELECTRICITY', 'HEAT', 'GAS', 'STEAM']
        self.aggregated_totals = {}

    def process_csv(self, bucket_name, file_key):
        logging.info("Processing CSV")
        response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body'].read().decode('utf-8')
        reader = csv.reader(io.StringIO(data))
        next(reader)
        for row in reader:
            logging.info(str.format("Start date - {}, End data - {}, category - {}, fuel type - {}, activity value- {}, activity unit - {}", row[0], row[1], row[2], row[3], row[4], row[5]))
            if row[2] in self.elist:
                partition_key_value = row[2]+"#"+row[5]+"#01/01/2024#31/12/2024"
            else:
                partition_key_value = row[3]+"#"+row[5]+"#01/01/2024#31/12/2024"
            response = self.query_db(partition_key_value)
            if 'Items' in response and len(response['Items']) > 0:
                total_kg_co2e_per_unit = response['Items'][0]['total_kg_co2e_per_unit']
                carbon_emission_value = float(row[4]) * float(total_kg_co2e_per_unit)
            boolV=self.aggregate_data(row,carbon_emission_value,self.aggregated_totals)
        logging.info("Aggregated Totals::",self.aggregated_totals )
        return self.aggregated_totals


    def aggregate_data(self, row, carbon_emission_value, aggregated_totals):
        if row[2] in self.elist:
            fuel_type = row[2]
        else:
            fuel_type = row[3]
        start_date = row[0]
    
        month, year = start_date.split('/')[1:]
        key = (fuel_type, int(month), int(year))
        if key in aggregated_totals:
            aggregated_totals[key] += carbon_emission_value
        else:
            aggregated_totals[key] = carbon_emission_value
        return True

    def write_to_dynamodb(self, aggregated_totals):
        hierarchical_data = {}
        for fuel_type, month, year in aggregated_totals.keys():
            fuel_type_data = hierarchical_data.setdefault(fuel_type, {})
            month_year = f"{datetime.date(year, month, 1).strftime('%B')} - {year}"
            fuel_type_data[month_year] = aggregated_totals[(fuel_type, month, year)]

            customer_id = str(uuid.uuid4())
            for fuel_type, data in hierarchical_data.items():
                for month_year, value in data.items():
                    item = {
                        'customer_id': {'S': customer_id},
                        'fuel_type': {'S': fuel_type},
                        'month_year': {'S': month_year},
                    '   value': {'N': str(value)}
                }
            self.dynamodb_client.put_item(
                TableName='energy_aggregated_table',
                Item=item
            )
        print("Data written to DynamoDB successfully!")
        return True
    
    def query_db(self, partition_key_value):
        table = self.dbresource.Table('energy_conversion_table')
        key_condition_expression = "#partition_key = :partition_key_val"
        expression_attribute_values = {
            ":partition_key_val": partition_key_value 
        }
        expression_attribute_names = {
            "#partition_key": "fueltype#unit#startdate#enddate"
        }
        response = table.query(
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names
        )
        return response