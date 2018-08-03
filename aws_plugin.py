# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink

# Importing base classes that we need to derive
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.decorators import apply_defaults 

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
                print('still pending...')
                time.sleep(5)



# Defining the plugin class
class CustomAwsPlugin(AirflowPlugin):
    name = "custom_aws_plugin"
    operators = [CloudwatchToS3Operator,S3DeletePrefixOperator,EmrOperator]
