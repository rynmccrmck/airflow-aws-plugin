from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink

from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.decorators import apply_defaults 

from io import StringIO
import logging
import time

class CloudwatchToS3Operator(BaseOperator):   
    template_fields = ('from_utc_timestamp','to_utc_timestamp','destination_prefix') 
    @apply_defaults
    def __init__(self, 
                 aws_conn_id,
                 task_name,
                 log_group_name,
                 log_stream_name_prefix,
                 from_utc_timestamp,
                 to_utc_timestamp,
                 destination_bucket, 
                 destination_prefix, **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.task_name = task_name
        self.log_group_name = log_group_name
        self.log_stream_name_prefix = log_stream_name_prefix
        self.from_utc_timestamp = from_utc_timestamp
        self.to_utc_timestamp = to_utc_timestamp
        self.destination_bucket = destination_bucket
        self.destination_prefix = destination_prefix

    def execute(self, context):
        logging.info("Executing CloudwatchToS3Operator")
        logging.info(', '.join("%s: %s" % item for item in vars(self).items()))
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        cloudwatch = aws.get_client_type('logs')
        response = cloudwatch.create_export_task(
            taskName=self.task_name,
            logGroupName=self.log_group_name,
            logStreamNamePrefix=self.log_stream_name_prefix,
            fromTime=int(self.from_utc_timestamp),
            to=int(self.to_utc_timestamp),
            destination=self.destination_bucket,
            destinationPrefix=self.destination_prefix)
        task_id = response['taskId']
        status_code = "RUNNING"
        while status_code == "RUNNING":
            time.sleep(2)
            status_code = cloudwatch.describe_export_tasks(taskId = task_id)['exportTasks'][0]['status']['code']

        if status_code != "COMPLETED":
            raise AirflowException('Cloudwatch export task failed -{}'.format(status_code))


class UploadFileToS3Operator(BaseOperator):
    template_fields = ('prefix','local_filename','s3_filename')
    @apply_defaults
    def __init__(self, 
                 aws_conn_id,
                 bucket_name,
                 prefix,
                 local_filename,
                 s3_filename, **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix 
        self.local_filename = local_filename
        self.s3_filename = s3_filename 
    def execute(self, context):
        logging.info("Executing UploadFileToS3Operator")   
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3 = aws.get_client_type('s3')
        s3_location = '{0}/{1}'.format(self.prefix, self.s3_filename)
        s3.upload_file(self.local_filename, self.bucket_name, s3_location)


class MySQLToS3Operator(BaseOperator):
    """"""
    template_fields = ('query','prefix','s3_filename')
    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 mysql_conn_id,
                 bucket_name,
                 prefix,
                 s3_filename,
                 query,
                 replace_s3_file=True,
                 headers=False,**kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix  
        self.s3_filename = s3_filename
        self.replace_s3_file = replace_s3_file
        self.headers = headers
        self.query = query
    
    def query_mysql(self):
        mysql = MySqlHook(self.mysql_conn_id)
        con = mysql.get_conn()
        cur = con.cursor()
        cur.execute(self.query)
        result = cur.fetchall()
        if self.headers:
            headers = [col[0] for col in cur.description]
            result = (tuple(headers,),) + result
        return result
    
    def copy_results_s3(self,results):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3 = aws.get_client_type('s3')
        concat = StringIO()
        [concat.write(",".join(map(str, i)) + '\n') for i in results]
        s3_location = self.prefix + '/' + self.s3_filename
        s3.put_object(Body=concat.getvalue(), Bucket=self.bucket_name, Key=s3_location)
    
    def execute(self, context):    
        logging.info("Executing MySQLToS3Operator")
        results = self.query_mysql()
        self.copy_results_s3(results)


class S3DeletePrefixOperator(BaseOperator):
    template_fields = ('prefix',)
    @apply_defaults
    def __init__(self, 
                 aws_conn_id,
                 bucket_name,
                 prefix, **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix

    def execute(self, context):
        logging.info("Executing S3DeletePrefixOperator")
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3 = aws.get_client_type('s3')
        objects_to_delete = s3.list_objects(Bucket=self.bucket_name, Prefix=self.prefix)
        delete_keys = {'Objects' : []}
        delete_keys['Objects'] = [{'Key' : k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
        try:
            response = s3.delete_objects(Bucket=self.bucket_name, Delete=delete_keys)
            logging.info(response)
        except Exception as e: # TODO import botocode client exception for missing delete
            logging.info('delete error {}'.format(e))
        pass

class EmrOperator(BaseOperator):
    template_fields = ('steps',)
    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 cluster_name,
                 log_s3_uri,
                 release_label,
                 instance_type,
                 instance_count,
                 ec2_key_name,
                 applications,
                 job_flow_role,
                 service_role,
                 configurations,
	         steps,
                 step_timeout_minutes,**kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.cluster_name = cluster_name
        self.log_s3_uri = log_s3_uri 
        self.release_label = release_label 
        self.instance_type = instance_type 
        self.instance_count = instance_count 
        self.ec2_key_name = ec2_key_name 
        self.applications = applications 
        self.job_flow_role = job_flow_role 
        self.service_role = service_role 
        self.configurations = configurations 
        self.steps = steps 
        self.step_timeout_minutes = step_timeout_minutes

    def execute(self, context):
        logging.info("Executing EmrOperator")
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        emr = aws.get_client_type('emr')
        # create the cluster
        response = emr.run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_s3_uri,
            ReleaseLabel=self.release_label,
            Instances={
                'MasterInstanceType': self.instance_type,
                'SlaveInstanceType': self.instance_type,
                'InstanceCount': self.instance_count,
                'KeepJobFlowAliveWhenNoSteps': False,
                'Ec2KeyName': self.ec2_key_name
            },
            Applications=self.applications,
            VisibleToAllUsers=True,
            JobFlowRole=self.job_flow_role,
            ServiceRole=self.service_role,
            Configurations=self.configurations,
        )
        # add steps
        cluster_id = response['JobFlowId']
        response_step = emr.add_job_flow_steps(
                            JobFlowId=cluster_id,
                            Steps=self.steps)

        start_time = time.time()

        while True:
            if self.step_timeout_minutes:
                if time.time() > start_time + self.step_timeout_minutes * 60:
                    raise AirflowException('EMR step(s) time out!')
            step_statuses = [i['Status']['State'] for i in emr.list_steps(ClusterId=cluster_id)['Steps']]
            if not any(map(lambda x: x in ('PENDING','RUNNING'), step_statuses)):
                if not all(map(lambda x: x == 'COMPLETED', step_statuses)):
                    raise AirflowException('EMR step(s) failed!')
                break
            else:
                logging.info("Job(s) still running/pending")
                time.sleep(60)


# Defining the plugin class
class CustomAwsPlugin(AirflowPlugin):
    name = "custom_aws_plugin"
    operators = [CloudwatchToS3Operator,S3DeletePrefixOperator,EmrOperator,MySQLToS3Operator,UploadFileToS3Operator]
