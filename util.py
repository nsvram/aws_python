
# coding: utf-8

# In[26]:

from datetime import datetime, timedelta
import traceback
import logging
import os 
import base64
import re
import glob
import subprocess
import pandas as pd
import hashlib
import configparser
import shutil
from ftplib import FTP



def test_mypy():
    return "Success"

# In[32]:

# Configuration Ini
def create_conf_section(config, conf_section):
    try:
        if not config.has_section(conf_section):
            config.add_section(conf_section)
    except Exception as e:
        logging.error(traceback.format_exc())

def set_config_val(conf_section,conf_key,conf_val,cfgfile="./config.ini"):
    """
    Update/Add configuration to ConfigParser configuration file defined by cfgfile.
    
    conf_section: Section in the configuration file. 
    conf_key: Configuration name.
    conf_val: Configuration value.
    cfgfile: Configuration file full path.
    """
    try:
        # Create config section if dne
        config = configparser.ConfigParser()
        config.read(cfgfile)
        create_conf_section(config,conf_section)

        # Update config file
        cfgfile = open(cfgfile,'w')
        config.set(conf_section,conf_key,conf_val)
        config.write(cfgfile)
        cfgfile.close()
    except Exception as e:
        logging.error(traceback.format_exc())

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





# Spark

def get_spark_session(master,appName,conf_list=[]):
    """ Outputs spark object which is the entry point for Spark Programming.
    
    master: Spark master address can be mesos or local. 
    
           For example, mesos://zk://10.7.144.132:2181,10.7.144.133:2181,10.7.144.134:2181/mesos or local[*]
    
    appName: Spark application name.
    
    conf_list: List of spark configuration that will be passed on to SparkConf().setAll().
    
    """
    try:
        if len(conf_list)==0:
            conf_list = [("spark.driver.memory","1g"),
                         ("spark.executor.memory","1g"),
                         ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
                         ("spark.mesos.coarse","false")]

        conf  = SparkConf().setAppName(appName).setMaster(master).setAll(conf_list)

        sc    = SparkContext(conf=conf)
        spark = SparkSession(sc)
        return spark 

    except Exception as e:
        logging.error(traceback.format_exc())

            
        
# Notification 
def create_attachment(dir_path, file_name, attachment_type = "text/plain"):
    try:
        file_name = "{0}/{1}".format(dir_path,file_name)
        with open(file_name, "rb") as f:
            attach_b64 = base64.b64encode(f.read())
            
        attachment = Attachment()
        attachment.set_content(attach_b64)
        attachment.set_type(attachment_type)
        attachment.set_filename(file_name)
        attachment.set_disposition("attachment")
        attachment.set_content_id(file_name)
        
        return attachment
    
    except Exception as e:
            logging.error(traceback.format_exc())

def send_email(frm,to,subj,msg,list_attachments = None, 
               sendgrid_apikey="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"):
    try:

        sg = sendgrid.SendGridAPIClient(apikey=sendgrid_apikey)
        from_email = Email(frm)
        subject = subj
        to_email = Email(to)
        content = Content("text/plain", msg)
        mail = Mail(from_email, subject, to_email, content)

        if not list_attachments == None:
            for attach in list_attachments:
                mail.add_attachment(attach)

        response = sg.client.mail.send.post(request_body=mail.get())
        return response

    except Exception as e:
            logging.error(traceback.format_exc())


# Pandas
def get_meansd_from_pd(pd, grpbby_str = "store_id", aggby_str = "tran_count" ):
    """
    Get mean and standard deviation from pandas by grouping a field and aggregating a numeric column.
    
    pd: Input pandas data frame.
    grpbby_str: Field name to group by.
    aggby_str:  Field name to derive mean and sd.
    
    """
    try:
        pd_summary         = pd.groupby([grpbby_str]).agg({aggby_str:{"mean":"mean","std":"std"}})
        pd_summary         = pd_summary.unstack().reset_index()
        pd_summary.columns = ["level_0","level_1",grpbby_str, "value"] 
        pd_summary         = pd_summary.pivot(grpbby_str,"level_1","value").reset_index()
        
        return pd_summary
    
    except  Exception as e:
        logging.error(traceback.format_exc())

# FTP 
def get_ftp_conn(ftp_server,ftp_usr,ftp_pass):
    """
    Returns ftp object instance.
    
    ftp_server: String. Ftp hostname or ip address
    ftp_usr: String. FTP server username.
    ftp_pass: String. FTP server password.    
    """
    try:
        ftp = FTP(ftp_server)
        ftp.login(ftp_usr,ftp_pass)
        return(ftp)
    except Exception as e:
        logging.error(traceback.format_exc())

def download_ftp_files(ftp,ftp_dir,out_dir,pattrn=".*",delete_file=False):
    """
    Download ftp file from ftp_dir to out_dir. Returns True if files are downloaded otherwise False.
    
    ftp: String. ftp connection generated from function get_ftp_conn.
    ftp_dir: String. Directory in remote ftp where the file to be downloaded is located. Without slashes in the end.
    out_dir: String. Directory where the file will be downloaded. Without slashes in the end.
    pattrn: String. Regular expression pattern. This can be used to filter the files to be downloaded.
    delete_file: Boolean. Flag to indicate whether to delete the files in the ftp server.
    
    """
    download_result = False
    try:
        pattern = re.compile(pattrn)
        ftp.cwd(ftp_dir)
        list_files = [fle for fle in ftp.nlst(ftp_dir) if re.match(pattern,fle)]
        
        # Download files in list_files
        for fname in list_files:
            retr_cmd = 'RETR {0}'.format(fname)
            tgt_fname = "{0}/{1}".format(out_dir,fname)
            f = open(tgt_fname,"wb")
            ftp.retrbinary(retr_cmd,f.write)
            f.close()
            if delete_file:
                ftp.delete(fname)
        
        download_result = True
        
    except Exception as e:
        logging.error(traceback.format_exc())
    
    return(download_result)

