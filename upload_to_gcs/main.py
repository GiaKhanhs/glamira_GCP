import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up BigQuery and Storage clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Replace with your BigQuery dataset and table names
DATASET_ID = "excellent-well-440808-a7"
TABLE_ID_GLAMIRA = "glamira"
CHUNK_SIZE = 10000  # Number of rows per chunk for parallel processing
MAX_WORKERS = 4  # Number of threads to use for processing

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    """Triggered by a change to a Cloud Storage bucket."""
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    try:
        # Load and process the file
        process_file(bucket_name, file_name)
    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        raise


def process_file(bucket_name, file_name):
    """Processes a JSON file and uploads data to BigQuery in parallel."""
    try:
        # Load JSON file from Google Cloud Storage
        print(f"Loading file {file_name} from bucket {bucket_name}")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Stream the file instead of loading it entirely into memory
        file_stream = blob.download_as_bytes()
        data = []
        for line in file_stream.decode('utf-8').splitlines():
            data.append(json.loads(line))

        print("Parsing and splitting data into chunks...")
        chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]

        print(f"Processing {len(chunks)} chunks in parallel with {MAX_WORKERS} threads...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_chunk = {executor.submit(upload_chunk_to_bigquery, chunk): chunk for chunk in chunks}
            for future in as_completed(future_to_chunk):
                try:
                    future.result()  # Ensure the chunk is processed successfully
                except Exception as e:
                    print(f"Error processing a chunk: {e}")

        print(f"Finished processing file {file_name}.")
    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        raise


def upload_chunk_to_bigquery(chunk):
    """Uploads a chunk of data to BigQuery."""
    try:
        # Transform data
        transformed_data = transform_data(chunk)

        # Define BigQuery table ID and job configuration
        table_id = f"{DATASET_ID}.{TABLE_ID_GLAMIRA}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("_id", "STRING"),
                bigquery.SchemaField("time_stamp", "INTEGER"),
                bigquery.SchemaField("ip", "STRING"),
                bigquery.SchemaField("user_agent", "STRING"),
                bigquery.SchemaField("resolution", "STRING"),
                bigquery.SchemaField("user_id_db", "STRING"),
                bigquery.SchemaField("device_id", "STRING"),
                bigquery.SchemaField("api_version", "STRING"),
                bigquery.SchemaField("store_id", "STRING"),
                bigquery.SchemaField("local_time", "STRING"),
                bigquery.SchemaField("show_recommendation", "BOOLEAN"),
                bigquery.SchemaField("current_url", "STRING"),
                bigquery.SchemaField("referrer_url", "STRING"),
                bigquery.SchemaField("email_address", "STRING"),
                bigquery.SchemaField("collection", "STRING"),
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("options", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("option_label", "STRING"),
                    bigquery.SchemaField("option_id", "STRING"),
                    bigquery.SchemaField("value_label", "STRING"),
                    bigquery.SchemaField("value_id", "STRING"),
                    bigquery.SchemaField("quality", "STRING"),
                    bigquery.SchemaField("quality_label", "STRING"),
                ]),
                bigquery.SchemaField("recommendation", "BOOLEAN"),
                bigquery.SchemaField("utm_source", "STRING"),
                bigquery.SchemaField("utm_medium", "STRING"),
            ],
        )

        # Load chunk into BigQuery
        print(f"Uploading chunk of {len(transformed_data)} rows to BigQuery...")
        job = bigquery_client.load_table_from_json(
            transformed_data,
            table_id,
            job_config=job_config
        )

        # Wait for job to complete
        job.result()
        print(f"Successfully uploaded chunk of {len(transformed_data)} rows.")
    except Exception as e:
        print(f"Error uploading chunk: {str(e)}")


def transform_data(data):
    """Transforms raw JSON data to match BigQuery schema."""
    transformed = []
    for item in data:
        transformed_item = {
            '_id': item.get('_id', {}).get('$oid', ''),
            'time_stamp': process_timestamp(item.get('time_stamp')),
            'ip': item.get('ip', ''),
            'user_agent': item.get('user_agent', ''),
            'resolution': item.get('resolution', ''),
            'user_id_db': item.get('user_id_db', ''),
            'device_id': item.get('device_id', ''),
            'api_version': item.get('api_version', ''),
            'store_id': item.get('store_id', ''),
            'local_time': process_local_time(item.get('local_time')),
            'show_recommendation': process_boolean(item.get('show_recommendation')),
            'current_url': item.get('current_url', ''),
            'referrer_url': item.get('referrer_url', ''),
            'email_address': item.get('email_address', ''),
            'collection': item.get('collection', ''),
            'product_id': item.get('product_id', ''),
            'options': process_options(item.get('option', [])),
            'recommendation': process_boolean(item.get('recommendation', None)),
            'utm_source': process_boolean(item.get('utm_source', None)),
            'utm_medium': process_boolean(item.get('utm_medium', None)),
        }
        transformed.append(transformed_item)
    return transformed


def process_timestamp(timestamp):
    """Convert timestamp to integer."""
    try:
        return int(timestamp)
    except (ValueError, TypeError):
        return None


def process_local_time(local_time):
    """Convert local_time to ISO format."""
    if not local_time:
        return None
    try:
        return datetime.strptime(local_time, "%Y-%m-%d %H:%M:%S").isoformat()
    except ValueError:
        return None


def process_boolean(value):
    """Convert boolean-like values."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == 'true'
    return None


def process_options(options):
    """Transform options into BigQuery-compatible format."""
    if not options:
        return []
    return [
        {
            'option_label': opt.get('option_label', ''),
            'option_id': opt.get('option_id', ''),
            'value_label': opt.get('value_label', ''),
            'value_id': opt.get('value_id', ''),
        }
        for opt in options
    ]
