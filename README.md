# GCS to BigQuery Dataflow Loader

This repo contains a Python script that reads a CSV file from Google Cloud Storage (GCS), parses it, and then streams the data into a BigQuery table using Apache Beam Dataflow.

## Requirements
- Python 3.6+
- Apache Beam Python SDK
- Google Cloud SDK

## Setup

1. Install the required packages: `pip install apache-beam[gcp] google-cloud-storage`
2. Authenticate with Google Cloud: `gcloud auth application-default login`
3. Configure config.json file in the same directory as the script, with the following structure:

```
{
  "project_id": "your-google-cloud-project-id",
  "region_id": "dataflow-region",
  "bucket_id": "your-gcs-bucket-id",
  "csv_file": "path/to/your/csv/file",
  "dataset_id": "your-bigquery-dataset-id",
  "table_id": "your-bigquery-table-id"
}
```

Replace the placeholder values with your actual Google Cloud project ID, GCS bucket ID, CSV file path, and BigQuery dataset and table IDs.

# Usage 

Run the script with the following command: `python gcs_to_bigquery.py`

By default, the script uses the `DirectRunner`. 

To use the `DataflowRunner`: `python gcs_to_bigquery.py --runner DataflowRunner`
