from pyspark.sql import SparkSession
import logging
import sys

sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
import config

# import time

# Start timer to record script running time
# star_time = time.time()

# Logging setup
class HandlerFilter():
    '''Class to filter handler based on message levels'''
    def __init__(self, level):
        '''
        Initialize HandleFilter object.
              
        Args:
            level: Level to filter handler with
        '''
        self.__level = level

    def filter(self, log_record):
        '''
        Filter log record based on level.
              
        Args:
            log_record: Log to filter
        '''

        return log_record.levelno == self.__level

# Logger setup (emit log records)
logger = logging.getLogger("data_uploading")
logger.setLevel(logging.INFO)

# Handler setup (send the log records to the appropriate destination)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

file_handler = logging.FileHandler("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Open-ended_Capstone\\data\\logs\\pipeline.log")
logger.addHandler(file_handler)

# Filter setup (based on the message level)
console_handler.addFilter(HandlerFilter(logging.INFO))
# file_handler.addFilter(HandlerFilter(logging.WARNING))

# Formatter setup (specify the layout of log records in the final output)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
file_handler.setFormatter(formatter)


# Start SparkSession (entry point to Spark)

uploading_session = SparkSession.builder.master("local[*]").appName('Data_Uploading').getOrCreate()
sc = uploading_session.sparkContext

# Configure Hadoop

hadoop_conf = sc._jsc.hadoopConfiguration()
access_key = config.a_k
secret_key = config.s_k
endpoint = "https://s3.us-west-1.amazonaws.com"

hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.endpoint", endpoint)

# sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")

# Read Parquet files into DataFrames and write them as Parquet files into S3 bucket
Fact_Sales = uploading_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Fact_Sales")
Fact_Sales.write.parquet("s3a://aaa-raw-data/Star_Schema_Tables/Fact_Sales")
rows = Fact_Sales.count()
logger.info(f"Parquet file 'Fact_Sales' was successfully loaded into DataFrame and uploaded as Parquet file into S3 bucket. {rows} rows loaded")

Dim_Products = uploading_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Dim_Products")
Dim_Products.write.parquet("s3a://aaa-raw-data/Star_Schema_Tables/Dim_Products")
rows = Dim_Products.count()
logger.info(f"Parquet file 'Dim_Products' was successfully loaded into DataFrame and uploaded as Parquet file into S3 bucket. {rows} rows loaded")

Dim_Sales_Channels = uploading_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Dim_Sales_Channels")
Dim_Sales_Channels.write.parquet("s3a://aaa-raw-data/Star_Schema_Tables/Dim_Sales_Channels")
rows = Dim_Sales_Channels.count()
logger.info(f"Parquet file 'Dim_Sales_Channels' was successfully loaded into DataFrame and uploaded as Parquet file into S3 bucket. {rows} rows loaded")

Dim_Dates = uploading_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Dim_Dates")
Dim_Dates.write.parquet("s3a://aaa-raw-data/Star_Schema_Tables/Dim_Dates")
rows = Dim_Dates.count()
logger.info(f"Parquet file 'Dim_Dates' was successfully loaded into DataFrame and uploaded as Parquet file into S3 bucket. {rows} rows loaded")


# Record script running time
# script_time = round(time.time() - star_time, 2)
# logger.info(f"Script runnig time was {script_time} secs")