
# coding: utf-8

# In[225]:


import boto3
import psycopg2
import datetime
import warnings
import configparser
import jaydebeapi
import json
import pandas as pd
warnings.filterwarnings("ignore")
import time
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


# In[256]:


config = configparser.ConfigParser()
config.read("/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/conf/config.ini")
#config.read("../conf/config.ini")

DATABASE     = config.get("redshift","DATABASE")
HOST         = config.get("redshift","HOST")
USER         = config.get("redshift","USER")
PASSWORD     = config.get("redshift","PASSWORD")
PORT         = config.get("redshift","PORT")
SCHEMA       = config.get("redshift","SCHEMA")

AWS_ACCESS_KEY_ID     = config.get("aws_cred","AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("aws_cred","AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client('s3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key= AWS_SECRET_ACCESS_KEY
    )

athena_client = boto3.client('athena')
s3_output_location = 's3://newsflow/tmp/'
s3_athena_csv_output = "s3://newsflow/tmp/41f59cf9-62fd-4d08-a65c-fb1781212b74.csv"
#s3://newsflow/tmp/41f59cf9-62fd-4d08-a65c-fb1781212b74.csv


# In[236]:


def copy_s3_folder(src_bucket_name, src_key, tgt_bucket_name):
    for files in s3_client.list_objects(Bucket=src_bucket_name)["Contents"]:
        if src_key in files['Key']:
            if "weekly_akagt_20180422000000_0000_part_00.gz" in files['Key']:
                print("Copying .... " +  str(files['Key']))
                copy_source = {
                    'Bucket': src_bucket_name,
                    'Key': str(files['Key'])
                                }
                s3_client.copy(copy_source, tgt_bucket_name, str(files['Key']))

def athena_create_table(in_db_name, sql_create):
    config = {'OutputLocation': s3_output_location}
    context = {'Database': 'newsflow'}
    return athena_client.start_query_execution(
                             QueryString =  sql_create, 
                             QueryExecutionContext = context,
                             ResultConfiguration=config)

def athena_get_query_execution(in_QueryExecutionId):
    return athena_client.get_query_execution(QueryExecutionId=in_QueryExecutionId)

def athena_trigger_query(in_QueryString, s3_output_location):
    return athena_client.start_query_execution(QueryString=in_QueryString, ResultConfiguration={'OutputLocation': s3_output_location})

def save_results_local(filepath, result_text):
    with open(filepath, 'w') as file:
        file.write(result_text)

        


# Local function - Create Athena table
def local_athena_create_table(db_name, create_table_script):
    # Create Table 
    response_json = athena_create_table(db_name,create_table_script)
    # Get the response / confirmation from AWS 
    while True:
        # check the results after 5 sec
        restuls = athena_get_query_execution(response_json["QueryExecutionId"])
        #print(restuls["QueryExecution"]["Status"]["State"])
        if restuls["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
            return "SUCCEEDED"
            break
        if restuls["QueryExecution"]["Status"]["State"] == "FAILED":
            return "FAILED"
            break
        else:
            time.sleep(5)
#athena_create_table("newsflow",create_table_script)


# In[262]:


# Local function - Run query in Athena
def athena_run_query(in_QueryString):
    response_json = athena_trigger_query(in_QueryString,s3_output_location)
    while True:
        # check the results after 5 sec
        restuls = athena_get_query_execution(response_json["QueryExecutionId"])
        #print(restuls["QueryExecution"]["Status"]["State"])
        if restuls["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
            return "SUCCEEDED|" + restuls["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
            break
        if restuls["QueryExecution"]["Status"]["State"] == "FAILED":
            return "FAILED"
            break
        else:
            time.sleep(5)
#athena_run_query(in_QueryString)


# In[263]:


# Local function - read s3 results from Athena and save it in local folder
s3_OutputLocation = s3_athena_csv_output
BUCKET = s3_OutputLocation.split("//")[1].split("/")[0]
KEY = "/".join(s3_OutputLocation.split("//")[1].split("/")[1:])

def save_s3_local(BUCKET,KEY, tgt_file_path):
    result = s3_client.get_object(Bucket=BUCKET, Key=KEY)
    # Read the object (not compressed)
    result_text = result["Body"].read().decode()
    # Save the results
    save_results_local(tgt_file_path,result_text)
    return "SUCCEEDED"
#save_s3_local(BUCKET, KEY, "athena_query_results")


# In[216]:


# Dags Def


# In[221]:


default_args = { 
    "owner":"sampathkumarv",
    "depends_on_past": False,
    "start_date" : datetime.datetime(2018,4,30),    
    "retries": 1,
    "retry_delay" : datetime.timedelta(minutes=5)
}

dag = DAG("airflow_athena_s3_local", schedule_interval= "0 19 * * *", default_args=default_args)


# In[222]:


# Create Tasks for dags


# In[265]:


# Params for tasks1 - copy_s3_2_s3_folder
src_bucket_name = "ncgau-sit-shared"
src_key = "ambiata/aka/20180422/"
tgt_bucket_name = "newsflow"

# Params for tasks2 - athena_create_table
with open('/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/dags/data/ambiata_head.json') as json_data:
    data = json.load(json_data)

a = "CREATE EXTERNAL TABLE IF NOT EXISTS newsflow.rawairflowpoc ("
for i in range(0,len(data["attributes"])):
    if "page_counts_4w" in data["attributes"][i]["name"].split(":")[1]:
        a = a + "page_counts_4w int, "
    if "page_counts_8w" in data["attributes"][i]["name"].split(":")[1]:
        a = a + "page_counts_8w int, "
    if "page_counts_12w" in data["attributes"][i]["name"].split(":")[1]:
        a = a + "page_counts_12w int, "
    else:
        if i == (len(data["attributes"])-1):
            a = a + "column_{}".format(i) +  " string"
        else:
            a = a + "column_{}".format(i) +  " string,"

a = a + """)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'  MAP KEYS TERMINATED BY 'undefined' 
LOCATION 's3://newsflow/ambiata/aka/20180422/';"""    

db_name ="newsflow"
create_table_script = a.replace("\n", "")


# Params for tasks3 - athena_run_query
in_QueryString = """
        SELECT  
        count(*) as total_counts,
        count(distinct(column_0)) as distinct_count,
        sum(page_counts_4w) as s_page_counts_4w, 
        sum(page_counts_8w) as s_page_counts_8w, 
        sum(page_counts_12w) as s_page_counts_12w
        FROM 
        newsflow.rawairflowpoc"""

# Params for task4 - save_s3_local
s3_OutputLocation = s3_athena_csv_output
BUCKET = s3_OutputLocation.split("//")[1].split("/")[0]
KEY = "/".join(s3_OutputLocation.split("//")[1].split("/")[1:])
tgt_file_path = "athena_query_results.csv"


# In[267]:


# Define Tasks


# In[266]:


task1 = PythonOperator(task_id="copy_s3_2_s3_folder",
                       python_callable = copy_s3_folder,
                       op_args = [src_bucket_name,src_key, tgt_bucket_name],
                       dag=dag)


task2 = PythonOperator(task_id="athena_create_table",
                       python_callable = local_athena_create_table,
                       op_args = [db_name,create_table_script],
                       dag=dag)


task3 = PythonOperator(task_id="athena_run_query",
                       python_callable = athena_run_query,
                       op_args = [in_QueryString],
                       dag=dag)

task4 = PythonOperator(task_id="save_s3_local",
                       python_callable = save_s3_local,
                       op_args = [BUCKET,KEY,tgt_file_path],
                       dag=dag)


# In[268]:


# Define Dependencies


# In[269]:


task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)

