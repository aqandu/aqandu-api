#!/usr/bin/env python3

import os
import json
import subprocess
from dotenv import load_dotenv
import google.cloud
from google.cloud import bigquery


REGION = 'us-west2'


def bq_create_dataset(dataset_name):
    client = bigquery.Client()
    dataset_id = f"{client.project}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION

    try:
        client.get_dataset(dataset_id)
        print("\tDataset {} already exists".format(dataset_id))
    except google.api_core.exceptions.NotFound:
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"\tCreating BigQuery dataset: {dataset_name}")
        print(f"\tCreated dataset {client.project}.{dataset.dataset_id}")


def bq_create_table(dataset_name, 
                    table_name, 
                    schema_filename):
    client = bigquery.Client()

    # Check for dataset and create if it doesn't exist
    bq_create_dataset(dataset_name)

    print(f'Creating BigQuery table: "{dataset_name}.{table_name}"')

    table_id = f"{client.project}.{dataset_name}.{table_name}"

    with open(schema_filename, 'r') as f:
        table_info = json.load(f)

    schema = []
    for field in table_info['schema']:
        schema.append(
            bigquery.SchemaField(
                field['name'], 
                field['type'], 
                mode=field['mode'], 
                description=field.get('description', None)
            )
        )

    table = bigquery.Table(table_id, schema=schema)

    if 'partition' in table_info:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=table_info['partition']
        )
    if 'cluster' in table_info:
        table.clustering_fields = table_info['cluster']

    try:
        table = client.create_table(table)  # Make an API request.
        print("\tCreated table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    except google.api_core.exceptions.Conflict:
        print("\tTable already exists")


# ----------------------------------------------------
# MENUCONFIG
# ----------------------------------------------------
if input("Run menuconfig? [y/n]: ") == 'y':
    subprocess.run(["menuconfig"])
load_dotenv(".config")


# ----------------------------------------------------
# SET PROJECT
# ----------------------------------------------------
if os.getenv('CONFIG_EXISTING_PROJECT') == 'y':
    subprocess.run(f"gcloud config set project {os.getenv('CONFIG_PROJECT_NAME')}".split(' '))


# ----------------------------------------------------
# ENABLE APIS
# ----------------------------------------------------
subprocess.run("gcloud services enable firestore.googleapis.com".split())
subprocess.run("gcloud services enable cloudscheduler.googleapis.com".split())
subprocess.run("gcloud services enable cloudfunctions.googleapis.com".split())
subprocess.run("gcloud services enable cloudbuild.googleapis.com".split())

# ----------------------------------------------------
# FIRESTORE
# ----------------------------------------------------
print('Creating Firestore database...')
if os.getenv('CONFIG_FIRESTORE') == 'y':
    # App Engine must be enabled first
    subprocess.run(f"gcloud app create --region {REGION}".split())

    # Create the database
    subprocess.run(f"gcloud firestore databases create --region {REGION}".split())


# ----------------------------------------------------
# BIGQUERY
# ----------------------------------------------------
print('Creating BigQuery "telemetry" database')
if os.getenv('CONFIG_BQ_TELEMETRY') == 'y':

    # ----------------------------------------------------
    # TABLE: TELEMETRY.TELEMETRY
    # ----------------------------------------------------
    bq_create_table(
        dataset_name='telemetry', 
        table_name='telemetry', 
        schema_filename='bq_telemetry_schema.json'
    )

    # ----------------------------------------------------
    # TABLE: META.DEVICES
    # ----------------------------------------------------
    bq_create_table(
        dataset_name='meta', 
        table_name='devices',
        schema_filename='bq_meta_devices_schema.json'
    )

    # ----------------------------------------------------
    # TABLE: INTERNAL.API_QUOTA_HISTORY
    # ----------------------------------------------------
    bq_create_table(
        dataset_name='internal', 
        table_name='api_quota_history',
        schema_filename='bq_internal_api_quota_history.json'
    )

    # ----------------------------------------------------
    # TABLE: INTERNAL.API_REQUEST_TRACKER
    # ----------------------------------------------------
    bq_create_table(
        dataset_name='internal', 
        table_name='api_request_tracker',
        schema_filename='bq_internal_api_request_tracker.json'
    )






