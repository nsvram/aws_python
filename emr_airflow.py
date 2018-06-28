from __future__ import division, absolute_import, print_function
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    "owner":"sampathkumarv",
    "depends_on_past": False,
    "start_date" : datetime(2018,4,30),
    "retries": 1,
    "retry_delay" : timedelta(minutes=5)
}

dag = DAG("emrAirflow", schedule_interval= "0 19 * * *", default_args=default_args)


def print_hello():
    print('Hello world!')
    try:
        print("eafds")
        create_emr = EmrCreateJobFlowOperator(task_id='create_job_flow',aws_conn_id='aws_default',dag=dag)
        return(create_emr)
    except AirflowException as ae:
        print (ae.message)


dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
dummy_operator >> hello_operator

