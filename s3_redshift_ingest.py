
# coding: utf-8

# In[25]:


import boto3
import psycopg2
import datetime
import warnings
import util
import configparser
import jaydebeapi
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
warnings.filterwarnings("ignore")


# In[26]:


config_file_path = "/".join(os.getcwd().split("/")[:-1]) + "/conf/config.ini"
config_file_path = '/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/conf/config.ini'
config = configparser.ConfigParser()
config.read(config_file_path)
DATABASE     = config.get("redshift","DATABASE")
HOST         = config.get("redshift","HOST")
USER         = config.get("redshift","USER")
PASSWORD     = config.get("redshift","PASSWORD")
PORT         = config.get("redshift","PORT")
SCHEMA       = config.get("redshift","SCHEMA")

AWS_ACCESS_KEY_ID     = config.get("aws_cred","AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("aws_cred","AWS_SECRET_ACCESS_KEY")


def get_s3_bucket():
    s3_client = boto3.client('s3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    response = s3_client.list_objects_v2(Bucket="ncgau-uat-shared")
    return "Success -- " + response['Contents'][2]['Key']
#get_s3_bucket()

    
def get_data():
    text2save = []
    try:
        conn=psycopg2.connect("dbname={0} host={1} port={2} user={3} password={4}".format(DATABASE,HOST,PORT,USER,PASSWORD))
    except:
        print ("I am unable to connect to the database {0}.".format(DATABASE))
        exit()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT * from events limit 2""")
    except:
        print ("I can't SELECT from bar")
    rows = cur.fetchall()
    for row in rows:
        text2save = text2save +  ([row[i] for i in range(0,len(row))])
    return text2save
    cur.close()
    conn.close()
    
#get_data()


# In[30]:


def return_status():
    return "Success"
#return_status()


# In[32]:


default_args = { 
    "owner":"sampathkumarv",
    "depends_on_past": False,
    "start_date" : datetime.datetime(2018,4,23),    
    "retries": 1,
    "retry_delay" : datetime.timedelta(minutes=5)
}

dag = DAG("s3_redshift_ingest", schedule_interval= "0 19 * * *", default_args=default_args)


# ## Define Tasks

# In[33]:


task1 = PythonOperator(task_id="get_data",
                       python_callable = get_data,
                       op_args = "",
                       dag=dag)
task2 = PythonOperator(task_id="get_s3_bucket",
                       python_callable = get_s3_bucket,
                       op_args = "",
                       dag=dag)
task3 = PythonOperator(task_id="return_status",
                       python_callable = return_status,
                       op_args = "",
                       dag=dag)


# ## Define Schedule

# In[34]:


task2.set_upstream(task1)
task3.set_upstream(task2)

