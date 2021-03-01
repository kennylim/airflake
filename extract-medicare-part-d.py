from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from dags.socrata.common.defaults import DATASET_ID, SOURCE_PATH, TARGET_PATH, DOMAIN
from dags.socrata.config.sql_utility import (
    MEDICARE_PART_D_PROVIDER_TRUNCATE_STAGE,
    MEDICARE_PART_D_PROVIDER_COPY_TO_STAGE,
    MEDICARE_PART_D_PROVIDER_LOAD_MASTER_TABLE,
    MEDICARE_PART_D_PROVIDER_LOAD_CREATE_VIEW,
    MEDICARE_PART_D_PROVIDER_OFF_SET_START,
    MEDICARE_PART_D_PROVIDER_OFF_SET_END,
    MEDICARE_PART_D_PROVIDER_TABLE_MAX_NPI,
    MEDICARE_PART_D_PROVIDER_FILE_MAX_NPI,
    MEDICARE_PART_D_PROVIDER_DOWNLOAD_FILE,
)

from sodapy import Socrata
from datetime import datetime, timedelta
from pathlib import Path

import json
import pandas as pd
#import pendulum
import yaml
import sys
import os
import jinja2
import requests
import re


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email": ["work@kennylim.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "provide_context": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="extract-medicare-part-d",
    description="extract data from socrata api based on dataset_id",
    default_args=default_args,
    schedule_interval=None,
    concurrency=10,
    catchup=False,
)

"""
    below is an example of test arguments to run tests with the dag:
    {
        "dataset_id": "gra9-xbjk",
    }
"""


def get_variable_from_context(context, key):
    context_values = {
        "dataset_id": context["dag_run"].conf.get("dataset_id", DATASET_ID),
        "source_path": context["dag_run"].conf.get("source_path", SOURCE_PATH),
    }
    return context_values[key]


def get_table_row_count(**context):
    table_row_count = MEDICARE_PART_D_PROVIDER_OFF_SET_START
    print("Counting TABLE MAX Row ...")
    dwh_hook = SnowflakeHook(snowflake_conn_id="kyu_snowflake_conn", warehouse="COMPUTE_WH")
    table_row_count = dwh_hook.get_first(table_row_count)[0]
    print("OFF_SET_START: " + str(table_row_count))


def get_file_row_count(**context):
    print("Counting FILE MAX Row ...")
    get_file_row_count = requests.get(MEDICARE_PART_D_PROVIDER_OFF_SET_END)
    #  get_file_row_count = requests.get('https://data.cms.gov/resource/77gb-8z53.csv?$select=count(*)')
    x = re.findall(r"\d+", get_file_row_count.text)
    total_row_count = x[0]
    print("OFF_SET_END: " + str(total_row_count))


def get_table_max_incremental_number(**context):
    get_table_max_incremental_number = MEDICARE_PART_D_PROVIDER_TABLE_MAX_NPI
    print("Get Table MAX increment number...")
    dwh_hook = SnowflakeHook(snowflake_conn_id="kyu_snowflake_conn", warehouse="COMPUTE_WH")
    dwh_hook.run(get_table_max_incremental_number, autocommit=True)
    table_max_npi = dwh_hook.get_first(get_table_max_incremental_number)[0]
    print("MAX Table increment number: " + str(table_max_npi))


def get_file_max_incremental_number(**context):
    max_file_incremental_number = requests.get("https://data.cms.gov/resource/77gb-8z53.csv?$select=max(npi)")
    x = re.findall(r"\d+", max_file_incremental_number.text)
    max_file_npi = x[0]
    print("MAX File increment number: " + str(max_file_npi))


# TODO:


# def get_file_name(**context):
#    print(1)


# def get_timestamp(**context):
#    print(2)


# def create_bucket(**context):
#    print(3)


def extract_file(**context):
    print("Downloading Medicare Part D Provider File...")
    os.system(MEDICARE_PART_D_PROVIDER_DOWNLOAD_FILE)
    print("Download File Complete")


def compress_file(**context):
    print("Compressing Downloaded File...")
    os.system("bzip2 -9 /usr/local/airflow/medical_provider_payment.csv")


def copy_file_to_datalake(**context):
    print("Copying downloaded file to AWS S3 Bucket...")
    os.system("aws s3 cp /usr/local/airflow/medical_provider_payment.csv.bz2 s3://medly/")


def truncate_stage_table(**context):
    truncate_stage_table = MEDICARE_PART_D_PROVIDER_TRUNCATE_STAGE
    print("Truncate staging table")
    dwh_hook = SnowflakeHook(snowflake_conn_id="kyu_snowflake_conn", warehouse="COMPUTE_WH")
    dwh_hook.run(truncate_stage_table, autocommit=True)


def stage_file(**context):
    stage_file = MEDICARE_PART_D_PROVIDER_COPY_TO_STAGE
    print("Loading S3 file into staging table")
    dwh_hook = SnowflakeHook(snowflake_conn_id="kyu_snowflake_conn", warehouse="COMPUTE_WH")
    dwh_hook.run(stage_file, autocommit=True)


def load_stage_to_master(**context):
    load_stage_to_master = MEDICARE_PART_D_PROVIDER_LOAD_MASTER_TABLE
    print("Loading stage to master...")
    dwh_hook = SnowflakeHook(snowflake_conn_id="kyu_snowflake_conn", warehouse="COMPUTE_WH")
    dwh_hook.run(load_stage_to_master, autocommit=True)


def create_or_replace_view(**context):
    create_or_replace_view = MEDICARE_PART_D_PROVIDER_LOAD_CREATE_VIEW
    print("creating or replacing existing view.")
    dwh_hook = SnowflakeHook(snowflake_conn_id="kyu_snowflake_conn", warehouse="COMPUTE_WH")
    dwh_hook.run(create_or_replace_view, autocommit=True)


# TODO:

# def archive_bucket_file
#    print(4)
# def update_job_history_metadata(**context):
#    print(5)

start = DummyOperator(task_id="start", dag=dag)

get_table_row_count = PythonOperator(
    provide_context=True, python_callable=get_table_row_count, task_id="get_table_row_count", dag=dag
)

get_file_row_count = PythonOperator(
    provide_context=True, python_callable=get_file_row_count, task_id="get_file_row_count", dag=dag
)

get_table_max_incremental_number = PythonOperator(
    provide_context=True,
    python_callable=get_table_max_incremental_number,
    task_id="get_table_max_incremental_number",
    dag=dag,
)

get_file_max_incremental_number = PythonOperator(
    provide_context=True,
    python_callable=get_file_max_incremental_number,
    task_id="get_file_max_incremental_number",
    dag=dag,
)

create_storage_bucket = DummyOperator(task_id="create_storage_bucket", dag=dag)

extract_file = PythonOperator(
    provide_context=True, python_callable=extract_file, task_id="parallel_file_extract", dag=dag
)

compress_file = PythonOperator(provide_context=True, python_callable=compress_file, task_id="compress_file", dag=dag)

copy_file_to_datalake = PythonOperator(
    provide_context=True, python_callable=copy_file_to_datalake, task_id="copy_file_to_datalake", dag=dag
)

truncate_stage_table = PythonOperator(
    provide_context=True, python_callable=truncate_stage_table, task_id="truncate_stage_table", dag=dag
)

stage_file = PythonOperator(provide_context=True, python_callable=stage_file, task_id="stage_file", dag=dag)

load_stage_to_master = PythonOperator(
    provide_context=True, python_callable=load_stage_to_master, task_id="load_stage_to_master", dag=dag
)

create_or_replace_view = PythonOperator(
    provide_context=True, python_callable=create_or_replace_view, task_id="create_or_replace_view", dag=dag
)

end = DummyOperator(task_id="end", dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)

start >> (
    get_table_row_count,
    get_file_row_count,
    get_table_max_incremental_number,
    get_file_max_incremental_number,
) >> create_storage_bucket >> extract_file >> compress_file >> copy_file_to_datalake >> truncate_stage_table >> stage_file >> load_stage_to_master >> create_or_replace_view >> end
