
import boto3
import warnings
import configparser
import json
import pandas as pd
warnings.filterwarnings("ignore")

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import *
from datetime import datetime, timedelta
from aws_lib import *
import time

config = configparser.ConfigParser()
config.read("/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/conf/config.ini")


AWS_ACCESS_KEY_ID     = config.get("aws_cred","AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("aws_cred","AWS_SECRET_ACCESS_KEY")

jobflow_id = ''


def emr_create_cluster():
    # Create cluster
    jobflow_id = create_cluster_5_12()
    f= open("/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/logs/jobflow_id.txt","w+")
    f.write(jobflow_id)
    f.close()
    return jobflow_id

def emr_terminate_cluster():
    jobflow_id = read_jobflow_id() 
    response = emr_client.terminate_job_flows(JobFlowIds=[jobflow_id,])
    return response


def emr_submit_steps_hdfs_copy_1():
   # EMR hadoop data copy
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_dist_cp_s3_hdfs_step("newsflow", "inbound_data"))
   #emr_check_stage_status()

def emr_submit_steps_hdfs_copy_2():
   # EMR hadoop data copy
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_dist_cp_s3_hdfs_step("newsflow", "event"))
   #emr_check_stage_status()

def emr_submit_steps_hdfs_copy_3():
   # EMR hadoop data copy
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_dist_cp_s3_hdfs_step("newsflow", "krux"))
   #emr_check_stage_status()

def emr_submit_steps_spark_job_1():
   # Spark jobs 
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_spark_step("s3://newsflow/code/emr_spark_read_psv.py"))
   #emr_check_stage_status()
 
def emr_submit_steps_spark_job_2():
   # Spark jobs 
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_spark_step("s3://newsflow/code/emr_spark_json.py"))
   #emr_check_stage_status()

def emr_submit_steps_spark_job_3():
   # Spark jobs 
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_spark_step("s3://newsflow/code/emr_spark_read_csv_krux.py"))
   #emr_check_stage_status()

def emr_submit_steps_hdfs_s3():
   # EMR to S3 
   jobflow_id = read_jobflow_id()
   emr_client.add_job_flow_steps(JobFlowId=jobflow_id,Steps=get_dist_cp_hdfs_s3_step('newsflow',"output/krux"))
   emr_check_status()
   #emr path - /newsflow/output/krux/
   # coutput s3 path -  /newsflow/emr_output/output/krux/


# airflow defalut args
today_date = datetime.datetime.today()
default_args = {
   'owner': 'sampathkumarv',
   'depends_on_past': False,
   'start_date': today_date,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=2),
}
dag = DAG('emr_spark_s3', default_args=default_args, schedule_interval= '@once')

# List of tasks to be performed
start_workflow = DummyOperator(task_id='start_workflow', retries=3, dag=dag)
emr_create_cluster = PythonOperator(task_id="emr_create_cluster", python_callable=emr_create_cluster, dag=dag)
emr_check_spin_up_status = PythonOperator(task_id="emr_check_spin_up_status", python_callable=emr_check_status,  dag=dag)

emr_submit_steps_hdfs_copy_1 = PythonOperator(task_id="emr_submit_steps_hdfs_copy_1", python_callable=emr_submit_steps_hdfs_copy_1,  dag=dag)
emr_submit_steps_hdfs_copy_2 = PythonOperator(task_id="emr_submit_steps_hdfs_copy_2", python_callable=emr_submit_steps_hdfs_copy_2,  dag=dag)
emr_submit_steps_hdfs_copy_3 = PythonOperator(task_id="emr_submit_steps_hdfs_copy_3", python_callable=emr_submit_steps_hdfs_copy_3,  dag=dag)

emr_submit_steps_spark_job_1 = PythonOperator(task_id="emr_submit_steps_spark_job_1", python_callable=emr_submit_steps_spark_job_1,  dag=dag)
emr_submit_steps_spark_job_2 = PythonOperator(task_id="emr_submit_steps_spark_job_2", python_callable=emr_submit_steps_spark_job_2,  dag=dag)
emr_submit_steps_spark_job_3 = PythonOperator(task_id="emr_submit_steps_spark_job_3", python_callable=emr_submit_steps_spark_job_3,  dag=dag)

emr_process_complete = DummyOperator(task_id='emr_process_complete', retries=3, dag=dag)

emr_check_job_completion_status = PythonOperator(task_id="emr_check_job_completion_status", python_callable=emr_check_status,  dag=dag)

emr_submit_steps_hdfs_s3 = PythonOperator(task_id="emr_submit_steps_hdfs_s3", python_callable=emr_submit_steps_hdfs_s3 , dag=dag)

data_validation = DummyOperator(task_id='data_validation', retries=3, dag=dag)
metadata_export = DummyOperator(task_id='metadata_export', retries=3, dag=dag)

emr_terminate_cluster = PythonOperator(task_id="emr_terminate_cluster", python_callable=emr_terminate_cluster,  dag=dag)


# Schedule the tasks
emr_create_cluster.set_upstream(start_workflow)
emr_check_spin_up_status.set_upstream(emr_create_cluster)

emr_submit_steps_hdfs_copy_1.set_upstream(emr_check_spin_up_status)
emr_submit_steps_hdfs_copy_2.set_upstream(emr_check_spin_up_status)
emr_submit_steps_hdfs_copy_3.set_upstream(emr_check_spin_up_status)
emr_submit_steps_spark_job_1.set_upstream(emr_check_spin_up_status)
emr_submit_steps_spark_job_2.set_upstream(emr_check_spin_up_status)
emr_submit_steps_spark_job_3.set_upstream(emr_check_spin_up_status)

emr_process_complete.set_upstream(emr_submit_steps_hdfs_copy_1)
emr_process_complete.set_upstream(emr_submit_steps_hdfs_copy_2)
emr_process_complete.set_upstream(emr_submit_steps_hdfs_copy_3)
emr_process_complete.set_upstream(emr_submit_steps_spark_job_1)
emr_process_complete.set_upstream(emr_submit_steps_spark_job_2)
emr_process_complete.set_upstream(emr_submit_steps_spark_job_3)

emr_check_job_completion_status.set_upstream(emr_process_complete)
emr_submit_steps_hdfs_s3.set_upstream(emr_check_job_completion_status)

data_validation.set_upstream(emr_submit_steps_hdfs_s3)
metadata_export.set_upstream(emr_submit_steps_hdfs_s3)

emr_terminate_cluster.set_upstream(data_validation)
emr_terminate_cluster.set_upstream(metadata_export)
