
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
# import apache.hadoop.fs.Path
from awsglue.job import Job
from pyspark.sql.functions import to_timestamp, to_date, date_format
import decimal
from datetime import datetime, date, timedelta
from botocore.exceptions import ClientError
import uuid
import logging
import boto3
import os
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from awsglue.gluetypes import *
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import psycopg2


spark = (SparkSession.builder
    .enableHiveSupport()
    .config("spark.sql.catalogImplementation", "hive")
    .config('spark.sql.hive.convertMetastoreParquet','false')
    .config("spark.sql.parquet.mergeSchema", "false")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .getOrCreate())

sc = spark.sparkContext
glue_context = GlueContext(sc)
logger = glue_context.get_logger()
print("Hadoop version: " + sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion())
sqlc = SQLContext(sc)

def get_argument(argument_name, default = '', raise_error=False):
    if (sys.argv.__contains__(f"--{argument_name}")):
        args = getResolvedOptions(sys.argv,[argument_name])
        return args[argument_name]
    if raise_error:
        raise Exception(f"Parameter {argument_name} is not defined")
    return default

def log(text):
    if env_mode == 'development':
        print(text)
        print('')
    logger.info(text)
    
env_mode = get_argument('RUN_MODE', 'development')
job_name = get_argument('JOB_NAME', 'Unnamed Job')
log(f"Starting JOB: {job_name}")

job = Job(glue_context)
job.init(job_name, {'JOB_NbAME': job_name})


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()


class DatabaseUploader():

    # Getting the access credentials in AWS S3. In our case we are going to access AWS Redshift
    red_path = 's3://YOURBUCKET/credentials/redshift.csv'
    cred = spark.read.options(header = 'True', delimiter = ';').csv(red_path)

    credencial = {}
    for campo in cred.select('campo').collect():
        credencial[f'{campo[0]}'] = cred.filter(col('campo') == f'{campo[0]}').select('chave').collect()[0][0]


    AK = credencial['access_key_id']
    SK = credencial['secret_access_key']

    host = credencial['host']
    user = credencial['user']
    password = credencial['password']
    iam = credencial['iam']
    access_key_id = AK
    secret_access_key = SK
    out_query = credencial['saida_query']  
    port = 5439
    connection_type = 'jdbc'
    db = 'redshift'


    #==================================================================================================================
    # Setting up the access

    s3 = boto3.resource('s3', region_name='sa-east-1')

    spark = SparkSession.builder.master('yarn').appName('Connect to redshift').enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sqlContext = HiveContext(sc)

    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyID", AK)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", SK)

    #==================================================================================================================

    def __init__(self, table_location=str, table_name=str, database=None, schema=str,
                query=None, region=None, extension=str, delimiter=None, write_mode=None):
        
        """
        The location, name and schema of the table are mandatory attributes to be filled in if it is necessary to send it to the DB via this script. It just doesn't apply
          if the query attribute is filled by some query. By default the database is YOUR DATABASE and can be changed if necessary.
          By default the writing attribute with PySpark comes predefined as overwrite, which can be changed if necessary, for example write_mode='append'.
          The attribute/parameter extension comes predefined as parquet, that is, the script assumes that your table is already in this format, in case it is necessary to change the extension
          for, for example, extension='csv' by default the reading format does not come with the attribute delimiter filled in, as it is a .csv file and there may be cases
          of tables with semicolon delimiter';' or just a comma','. Fill the attribute with the correct delimiter when reading files in .csv, EX. delimiter=';'.
        
          Methods currently available:
          - spark_read() Reads the table for preview.
          - data_upload() Sends the table or creates a query in the DB.
          - db_table_check() Returns the result of a query with the first 10 data records in the DB.
          - drop_table() Delete/Drops the table if necessary.
          - truncate_table() Truncate/Remove rows and keep the schema.
          - get_schema() Returns the schema passed to the attribute when instantiating it.
          - get_region() Return the region.
          - get_lake() Returns the name of the database.
          - set_region() Change the region if necessary.
          - get_table_info() Returns a description of the table.
          - get_query_info() Returns the query passed to the attribute when instantiating it.

        """
        
        self.table_location = table_location
        self.table_name = table_name
        self.database = database
        self.schema = schema
        self.query = query
        self.region = region
        self.extension = extension
        self.delimiter = delimiter
        self.write_mode = write_mode


        if self.database is None:
            self.database = 'YOUR DATABASE'
        else:
            self.database = database

        if self.region is None:
            self.region = 'YOUR REGION' 
        else:
            self.region    

        if self.query is not None:
            self.table_location = None
            self.table_name = table_name

        if self.write_mode is None:
            self.write_mode = 'overwrite'
        else:
            self.write_mode = write_mode        
        

    def spark_read(self):
        # Getting data from S3
        if self.extension == 'parquet':
            LOCATION = self.table_location
            tabela = spark.read.format(self.extension).load(LOCATION)
        
        elif self.extension == 'csv':
            LOCATION = self.table_location
            tabela = spark.read.format(self.extension)\
                .option('header', True)\
                .option('delimiter', self.delimiter)\
                .load(LOCATION)
        return tabela
    

    def data_upload(self):
        """
        Upload the name of the table received by the attribute to the database selected at the beginning of the class.
        If there is any Query, execute the query and commit, closing the connection with the database.
        """

        # Connecting with psycopg2
        con=psycopg2.connect(dbname = self.database , host=DatabaseUploader.host,
        port= DatabaseUploader.port, user= DatabaseUploader.user, password= DatabaseUploader.password)
        
        if self.query is None:
            dbtable = self.schema + '.' + self.table_name
            url = f'{DatabaseUploader.connection_type}:{DatabaseUploader.db}://' + DatabaseUploader.host + f':{DatabaseUploader.port}/{self.database}?user=' + DatabaseUploader.user + f'&password=' + DatabaseUploader.password
            
            (DatabaseUploader.spark_read(self).write
                .format("com.databricks.spark.redshift")
                .option("aws_iam_role", DatabaseUploader.iam)
                .option("url", url)
                .option("dbtable", dbtable)
                .option("tempdir", DatabaseUploader.out_query)
                .mode(f"{self.write_mode}")
                .save())
            
        # Uploads query
        else:
            if self.query is not None:
                cur = con.cursor()
                cur.execute(self.query)
                con.commit()

                cur.close() 
                con.close()


    # Checking table upload
    def db_table_check(self):

        """
        Returns a query with the first 10 records of the table in the database  
        """

        con=psycopg2.connect(dbname = self.database , host=DatabaseUploader.host,
        port= DatabaseUploader.port, user= DatabaseUploader.user, password= DatabaseUploader.password)

        cur = con.cursor()
        query_db = f'SELECT * FROM {self.schema}.{self.table_name} LIMIT 10'
        cur.execute(query_db)
        result = cur.fetchall()
        
        print(f'Data at {DatabaseUploader.db} local: {query_db}')
        for f in result:
            print(f)

        cur.close()

    def drop_table(self):
        """
        Deletes/Drops the table that was uploaded to the Bank
        """
        con=psycopg2.connect(dbname = self.database , host=DatabaseUploader.host,
        port= DatabaseUploader.port, user= DatabaseUploader.user, password= DatabaseUploader.password)
        cur = con.cursor()
        query_db = f'DROP TABLE IF EXISTS {self.table_name}'
        cur.execute(query_db)
        con.commit()
        cur.close() 

    def truncate_table(self):

        """
        Truncate/Remove rows from the table uploaded to the database
        """
        con=psycopg2.connect(dbname = self.database , host=DatabaseUploader.host,
        port= DatabaseUploader.port, user= DatabaseUploader.user, password= DatabaseUploader.password)
        cur = con.cursor()
        query_db = f'TRUNCATE TABLE IF EXISTS {self.table_name}'
        cur.execute(query_db)
        con.commit()
        cur.close() 

        
    def get_schema(self):
        """
        Shows defined schema
        """
        if self.query is not None:
            return 'Defined Schema'
        else:
            return self.schema 

    def get_lake(self):
        return self.database   
    
    def get_region(self):
        return self.region
    
    def set_region(self, regiao=str):
        """
        Changing the region
        """
        self.region = regiao

    def get_table_name(self):
        """
        Gets the table name
        """
        return self.table_name
    
    def get_table_info(self):
        """
        Uses pyspark's describe method for some table info
        """
        return self.spark_read().describe()
    
    def get_query_info(self):
        return self.query
