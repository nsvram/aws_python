import warnings
import boto3
import psycopg2
import datetime
import warnings
import configparser
import jaydebeapi
import json
import pandas as pd
import time
import csv
warnings.filterwarnings("ignore")

from io import StringIO
from airflow import DAG
from airflow.operators import BashOperator, S3KeySensor, EmailOperator, PythonOperator
from airflow.operators import redshift_to_s3_operator
#import RedshiftOperator

config = configparser.ConfigParser()
config.read("/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/conf/config.ini")

DATABASE     = config.get("redshift","DATABASE")
HOST         = config.get("redshift","HOST")
USER         = config.get("redshift","USER")
PASSWORD     = config.get("redshift","PASSWORD")
PORT         = config.get("redshift","PORT")
SCHEMA       = config.get("redshift","SCHEMA")

AWS_ACCESS_KEY_ID     = config.get("aws_cred","AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("aws_cred","AWS_SECRET_ACCESS_KEY")

def airflow_test():
    return "Task airflow_test completed"

# Dags

default_args = { 
    "owner":"sampathkumarv",
    "depends_on_past": False,
    "start_date" : datetime.datetime(2018,4,30),    
    "retries": 1,
    "retry_delay" : datetime.timedelta(minutes=5)
}

dag = DAG("RedshiftToS3Transfer_operators", schedule_interval= "0 19 * * *", default_args=default_args)

task1 = PythonOperator(task_id="airflow_test",
                       python_callable = airflow_test,
                       dag=dag)

task2 = redshift_to_s3_operator.RedshiftToS3Transfer(task_id="RedshiftToS3Transfer",
                             schema = "atomic",
                             table  = "aus_holiday",
                             s3_bucket = "newsflow",
                             s3_key = "inbound_data/australianpublicholidays.csv",
                             redshift_conn_id = "test_conn_redshift",
                             delimiter=",",
			     s3_conn_id = "test_conn_S3",
                             dag=dag)

task2.set_upstream(task1)

