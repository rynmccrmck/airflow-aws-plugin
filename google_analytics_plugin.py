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

from googleads import adwords
import sys

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd


class GoogleAnalyticsToS3Operator(BaseOperator):   
    template_fields = ('start','end','s3_filename') 
    @apply_defaults
    def __init__(self, 
                 aws_conn_id,
                 key_file_location,
                 view_id,
                 metrics,
                 dimensions,
                 start,
                 end,
                 s3_filename,
                 destination_bucket, 
                 destination_prefix, **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.key_file_location = key_file_location
        self.view_id = view_id
        self.metrics = metrics
        self.dimensions = dimensions
        self.start = start
        self.end = end
        self.s3_filename = s3_filename
        self.destination_bucket = destination_bucket
        self.destination_prefix = destination_prefix

    def execute(self, context):
        logging.info("Executing GoogleAnalyticsToS3Operator")
        logging.info(', '.join("%s: %s" % item for item in vars(self).items()))
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3 = aws.get_client_type('s3')
        s3_location = '{0}/{1}'.format(self.destination_prefix, self.s3_filename)

        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_file_location, SCOPES)
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        df = pd.DataFrame()
        PAYLOAD_SIZE = 100000
        
        names = None
        pagination = 0
        while True:
        #for page in range(MAX_RECORXDS//PAYLOAD_SIZE):
            payload = {
              'viewId': self.view_id,
              'dateRanges': [{'startDate': self.start, 'endDate': self.end}],
              'metrics': [{'expression': 'ga:sessions'},{'expression': 'ga:pageviews'}],
              'dimensions': [{'name': 'ga:campaign'},{'name': 'ga:medium'},
                             {'name': 'ga:source'},{'name': 'ga:region'},
                             {'name': 'ga:city'},{'name': 'ga:dateHourMinute'},
                             {'name': 'ga:deviceCategory'}],
              "orderBys":[
                          {"fieldName":"ga:sessions",
                           "sortOrder": "DESCENDING"}
                         ],
              "filtersExpression": 'ga:country=~Canada',
              "pageToken": "{}".format(pagination),
              'pageSize': PAYLOAD_SIZE
              }
            report = analytics.reports().batchGet(
                       body={
                             'reportRequests': [payload]
                            }).execute()
            if not names:
                headers = report['reports'][0]['columnHeader']
                dims = headers['dimensions']
                metrics = [i['name'] for i in headers['metricHeader']['metricHeaderEntries']]
                names = [i.replace('ga:','') for i in dims + metrics]
                
            if 'rows' in report['reports'][0]['data'].keys():
                data = pd.DataFrame([i['dimensions'] + i['metrics'][0]['values'] 
                                        for i in report['reports'][0]['data']['rows']],
                                    columns=names)
                df = df.append(data)
            else:
                break
            df['dateHourMinute'] = pd.to_datetime(df['dateHourMinute'],format='%Y%m%d%H%M').dt.tz_localize('UTC',ambiguous='infer').dt.tz_convert('UTC').dt.strftime('%Y-%m-%d %H:%M:00')
            local_file = '/tmp/temp_ga_data.csv'
            df.to_csv(local_file,index=False) 
            s3.upload_file(local_file,self.destination_bucket, s3_location)
            pagination += PAYLOAD_SIZE


class GoogleAdwordsToS3Operator(BaseOperator):
    template_fields = ('query_start_date','query_end_date','s3_filename')
    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 report_name,
                 fields,
                 query_start_date,
                 query_end_date,
                 conditions,
                 s3_filename,
                 destination_bucket,
                 destination_prefix,
                 yaml_file_location='~/',
                 temp_localfile='/tmp/deleteme_adwords.csv',
                 **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.report_name = report_name
        self.fields = fields
        self.query_start_date = query_start_date
        self.query_end_date = query_end_date
        self.conditions = conditions
        self.s3_filename = s3_filename
        self.destination_bucket = destination_bucket
        self.destination_prefix = destination_prefix
        self.yaml_file_location = yaml_file_location
        self.temp_localfile = temp_localfile
        
    def execute(self, context):
        logging.info("Executing {}".format(self.__class__.__name__))
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3 = aws.get_client_type('s3')
        s3_location = '{0}/{1}'.format(self.destination_prefix, self.s3_filename)
        local_file = '/tmp/temp_adwords_data.csv'
        s3_location = '{0}/{1}'.format(self.destination_prefix, self.s3_filename)
        
        # initiliaze adwords client
        client = adwords.AdWordsClient.LoadFromStorage(self.yaml_file_location)
        report_downloader = client.GetReportDownloader(version='v201806')
        
        # build awql report
        report_query = adwords.ReportQueryBuilder().\
                       Select(*self.fields).\
                       From(self.report_name).\
                       During(start_date=self.query_start_date, 
                              end_date=self.query_end_date)
                    
        for condition in self.conditions:
            report_query = report_query.Where(condition['name'])\
                                       .In(*condition['values'])

        report_query = report_query.Build()
        
        # Download report locally (temp)
        filepath = self.temp_localfile
        with open(filepath, 'wb') as handler:
            report_downloader.DownloadReportWithAwql(
                report_query, 'CSV', output=handler,
                skip_report_header=True,
                skip_column_header=False, 
                skip_report_summary=True,
                include_zero_impressions=False)
        #Upload to S3
        s3.upload_file(filepath,self.destination_bucket, s3_location)



class CustomGoogleAnalyticsPlugin(AirflowPlugin):
    name = "custom_google_analytics_plugin"
    operators = [GoogleAnalyticsToS3Operator,GoogleAdwordsToS3Operator]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []

