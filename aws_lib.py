import boto3
import psycopg2
import datetime
import warnings
import configparser
import jaydebeapi
warnings.filterwarnings("ignore")
import time

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


region_name="ap-southeast-2"
emr_client = boto3.client('emr',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key= AWS_SECRET_ACCESS_KEY, region_name="ap-southeast-2"
                )

# ---------------------------------------
#  EMR - Get instance id 
# ---------------------------------------

def get_emr_instances():
    clusters = emr_client.list_clusters()
    #clusters_id = [c["Id"] for c in clusters["Clusters"]  if c["Status"]["State"] in ["RUNNING", "WAITING"]]
    clusters_id = [c["Id"] for c in clusters["Clusters"]]
    return clusters_id


# ---------------------------------------
#  EMR Step - Copy data from s3 to s3
# ---------------------------------------
def get_dist_cp_step(bucket_name, source_path, target_path):
    return [{
        'Name': 'S3DistCp step',
        'ActionOnFailure': 'CANCEL_AND_WAIT',#'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE',
        'HadoopJarStep': {
            'Jar': '/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar',
            'Args': [
                '--src',
                's3://{0}/{1}'.format(bucket_name, source_path),
                '--dest',
                's3://{0}/{1}'.format(bucket_name, target_path),
                '--targetSize',
                '2048',
                '--srcPattern',
                '.*\.csv',
            ]
        }
    }]

# ---------------------------------------
#  EMR Step - Copy data from S3 to HDFS 
# ---------------------------------------
def get_dist_cp_s3_hdfs_step(bucket_name, source_path):
    return [{
        'Name': 'S3DistCp step',
        'ActionOnFailure': 'CANCEL_AND_WAIT',#'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [ 
                's3-dist-cp',
                '--src',
                's3://{0}/{1}'.format(bucket_name, source_path),
                '--dest',
                '/{0}/{1}'.format(bucket_name, source_path),
                '--targetSize',
                '2048',
            ]
        }
    }]
# ---------------
# Create Cluster emr-5.12.0
# ---------------
def create_cluster_5_12():
    response = emr_client.run_job_flow(
            Name="airflow-poc",
            LogUri="s3://newsflow/logs/",
            ReleaseLabel="emr-5.12.0",
            JobFlowRole='EMR_EC2_DefaultRole',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'emr-master',
                        'InstanceRole': 'MASTER',
                        'InstanceType': "r3.8xlarge",
                        'InstanceCount': 1,
                            
                    },
                    {
                        'Name': 'emr-core',
                        'InstanceRole': 'CORE',
                        'InstanceType': "r3.8xlarge",
                        'InstanceCount': 2,
                    },
                ],
                'Ec2KeyName': "discover-uat",
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': "subnet-dd2dc8b9",
                'ServiceAccessSecurityGroup': "sg-015f6e65",
                'EmrManagedMasterSecurityGroup': "sg-065f6e62",
                'AdditionalMasterSecurityGroups': ["sg-c472f1a0","sg-fa72f19e"],
                'EmrManagedSlaveSecurityGroup': "sg-075f6e63",
                'AdditionalSlaveSecurityGroups': ["sg-c472f1a0","sg-fa72f19e"],
            },
        
            Steps=[],
        
            Applications=[
                {
                    'Name': 'Hadoop'
                },
                {
                    'Name': 'Spark'
                },
            ],
            Configurations=[
                {
                    'Classification': 'Spark',
                    'Properties': {
                        'maximizeResourceAllocation': "true"
                    },
                    "Configurations" : []
                },
            ],
            VisibleToAllUsers=True,
            ServiceRole='EMR_DefaultRole',
    )

    #emr_client.add_job_flow_steps(JobFlowId=response['JobFlowId'],Steps=get_dist_cp_step("newsflow", "inbound_data/", "outbound_data/"))
    #emr_client.add_job_flow_steps(JobFlowId=response['JobFlowId'],Steps=get_dist_cp_s3_hdfs_step("newsflow", "inbound_data"))
    return response['JobFlowId']   

# Check the Cluster status 
def check_cluster_status(region_name, jobflow_id):
    response = emr_client.list_clusters()
    for cluster in response['Clusters']:
        if cluster['Id'] == jobflow_id:
            try:
                if(cluster['Status']['StateChangeReason']['Message']):
                    return (cluster['Status']['State'] , cluster['Status']['StateChangeReason']['Message'])
            except KeyError:
                return (cluster['Status']['State'], None)
    return None

# ---------------
# Create Cluster emr-5.13.0
# ---------------
def create_cluster():
    response = emr_client.run_job_flow(
            Name="NewsFlow",
            LogUri="s3://newsflow/logs/",
            ReleaseLabel="emr-5.13.0",
            JobFlowRole='EMR_EC2_DefaultRole',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'emr-master',
                        'InstanceRole': 'MASTER',
                        'InstanceType': "r3.8xlarge",
                        'InstanceCount': 1,
                            
                    },
                    {
                        'Name': 'emr-core',
                        'InstanceRole': 'CORE',
                        'InstanceType': "r3.8xlarge",
                        'InstanceCount': 2,
                    },
                ],
                'Ec2KeyName': "discover-uat",
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': "subnet-dd2dc8b9",
                'ServiceAccessSecurityGroup': "sg-015f6e65",
                'EmrManagedMasterSecurityGroup': "sg-065f6e62",
                'AdditionalMasterSecurityGroups': ["sg-c472f1a0","sg-fa72f19e"],
                'EmrManagedSlaveSecurityGroup': "sg-075f6e63",
                'AdditionalSlaveSecurityGroups': ["sg-c472f1a0","sg-fa72f19e"],
            },
        
            Steps=[],
        
            Applications=[
                {
                    'Name': 'Ganglia'
                },
                {
                    'Name': 'Spark'
                },
                {
                    "Name":"Zeppelin"
                },
            ],
            Configurations=[
                {
                    'Classification': 'spark-hive-site',
                    'Properties': {
                        "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    },
                    "Configurations" : []
                },
            ],
            VisibleToAllUsers=True,
            ServiceRole='EMR_DefaultRole',
    )

    return response['JobFlowId']   


def get_spark_step(exec_code):
    return [{
        'Name': 'spark step',
        'ActionOnFailure': 'CANCEL_AND_WAIT',#'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [ 
                'spark-submit',
                '--deploy-mode',
                "cluster", 
                '--driver-memory', 
                '2g', 
                '--executor-memory' ,
                '6g', 
                '--executor-cores',
                '2',
                '--num-executors',
                '90',
                '{0}'.format(exec_code),
                ]
        }
    }]


# ------------------------------------------
#  EMR Step - Copy data from EMR HDFS to S3
# ------------------------------------------
def get_dist_cp_hdfs_s3_step(bucket_name, source_path):
    return [{
        'Name': 'S3DistCp step',
        'ActionOnFailure': 'CANCEL_AND_WAIT',#'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [ 
                's3-dist-cp',
                '--src',
                '/{0}/{1}'.format(bucket_name, source_path),
                '--dest',
                's3://{0}/emr_output/{1}'.format(bucket_name ,source_path),
                '--targetSize',
                '2048',
            ]
        }
    }]



# ------------------------------------------
#  EMR Check the Cluster Status 
# ------------------------------------------
def emr_check_status():
    jobflow_id = read_jobflow_id()
    while True:
        response = emr_client.list_clusters()
        print(jobflow_id)
        for cluster in response['Clusters']:
            if cluster['Id'] == jobflow_id:
                print("Stage_1")
                if cluster['Status']['State'] == "TERMINATED" or cluster['Status']['State'] == 'TERMINATING':
                    print("Stage_2")
                    raise Exception('Cluster TERMINATED')
                if cluster['Status']['State'] == "STARTING":
                    print("Stage_3")
                    time.sleep(30)
                if cluster['Status']['State'] == "WAITING":
                    print("Stage_4")
                    return "WAITING"


# ------------------------------------------
#  EMR read job flow id 
# ------------------------------------------
def read_jobflow_id():
    f= open("/Users/sampathkumarv/news/venkat/airflow/workspace/airflow_home/logs/jobflow_id.txt","r+")
    jobflow_id = f.read()
    f.close()
    return jobflow_id


# ------------------------------------------
#  EMR check jobs stage 
# ------------------------------------------
def emr_check_stage_status():
    sleep_seconds = 10
    num_attempts = 0
    max_attempts=2
    emr_cluster_id = read_jobflow_id()
    while True:
        response = emr_client.list_steps(ClusterId=emr_cluster_id,StepStates=['PENDING', 'CANCEL_PENDING', 'RUNNING'])
        num_attempts += 1
        active_aws_emr_steps = response['Steps']

        if active_aws_emr_steps:
            if 0 < max_attempts <= num_attempts:
                raise Exception('Max attempts exceeded:\n' + json.dumps(response, indent=3, default=str))
            time.sleep(sleep_seconds)
        else:
            return    

