import argparse
import logging
import csv
import io
import json
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from google.cloud import storage

def parse_csv(file):
    reader = csv.reader(file)
    fieldnames = next(reader) # Read the header (first row)
    # Get CSV header schema
    schema = ','.join([f'{field}:STRING' for field in fieldnames])

    # Read the rest of the rows as data
    data = [dict(zip(fieldnames, row)) for row in reader]

    return data, schema

def read_csv_from_gcs(bucket_name, gcs_file_path):
    # Create a storage client
    storage_client = storage.Client()

    # Get the bucket and the blob
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)

    # Read the file content as a string
    file_content = blob.download_as_text(encoding='utf-8-sig')

    # Use StringIO to read the content as a file-like object
    with io.StringIO(file_content) as file:
        data, schema = parse_csv(file)

    return data, schema

def run_pipeline(runner, project_id, region_id, bucket_id, dataset_id, table_id, table_schema, data):
    # Set up pipeline options
    options = PipelineOptions(
        project=project_id,
        temp_location=f'gs://{bucket_id}/temp',
        region=region_id,  # Set the Dataflow job region. Must be the same as your GCS and bigquery dataset
    )
    
    # Create the pipeline
    with beam.Pipeline(options=options) as pipeline:
        
        # Use the specified runner
        if runner == 'DataflowRunner':
            options.view_as(beam.options.pipeline_options.GoogleCloudOptions).job_name = 'insert-rows-to-bigquery'
            options.view_as(beam.options.pipeline_options.GoogleCloudOptions).staging_location = f'gs://{bucket_id}/staging'
            options.view_as(beam.options.pipeline_options.GoogleCloudOptions).setup_file = './setup.py'
            runner = beam.runners.DataflowRunner(options=options)
        else:
            runner = beam.runners.DirectRunner()
        
        # Set the full biq query table path
        bigquery_path = f'{project_id}:{dataset_id}.{table_id}'
        
        # Define the pipeline
        (
            pipeline
            | 'Create data' >> beam.Create(data)
            | 'Write to BigQuery' >> WriteToBigQuery(
                bigquery_path,
                # schema='field1:STRING,field2:INTEGER,field3:FLOAT',
                schema=table_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        # Add logging to show when the pipeline starts and ends
        logging.info('Pipeline started.')
        pipeline_result = pipeline.run()
        pipeline_result.wait_until_finish()
        logging.info('Pipeline finished.')

    print(f'Job submitted to insert rows into {table_id}.')

if __name__ == '__main__':
    # Set the logging configuration
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    # Read command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        help='Runner to use: DataflowRunner or DirectRunner'
    )
    known_args, pipeline_args = parser.parse_known_args()

    # Load the comfig file
    with open('config.json') as f:
        config = json.load(f)
    
    # Replace with your Google Cloud project ID
    project_id = config['project_id']
    region_id = config['region_id']
    bucket_id = config['bucket_id']

    # Replace with your dataset, table IDs, and CSV file path
    csv_file = config['csv_file']
    dataset_id = config['dataset_id']
    table_id = config['table_id'] # table will be created if needed
    
    # Call the parse_csv function to get the data and schema
    data, table_schema = read_csv_from_gcs(bucket_id, csv_file)
    print(f'CSV FILE CONTAINS : {table_schema} => {data}') 
                                    
    # Call the main function
    run_pipeline(known_args.runner,
                 project_id, region_id, bucket_id, dataset_id, table_id, table_schema, data)
