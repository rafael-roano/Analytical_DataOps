from pyspark.sql import SparkSession
import logging
import sys
import time

sys.path.append("/usr/local/spark/resources/x/")
import config


# Start timer to record script running time
star_time = time.time()

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
logger = logging.getLogger("daily_data_uploading")
logger.setLevel(logging.INFO)

# Handler setup (send the log records to the appropriate destination)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

file_handler = logging.FileHandler("/usr/local/spark/resources/pipeline.log")
logger.addHandler(file_handler)

# Filter setup (based on the message level)
console_handler.addFilter(HandlerFilter(logging.INFO))
# file_handler.addFilter(HandlerFilter(logging.WARNING))

# Formatter setup (specify the layout of log records in the final output)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
file_handler.setFormatter(formatter)


# Start SparkSession (entry point to Spark)

uploading_session = SparkSession.builder.master("spark://spark:7077").appName('Data_Uploading').getOrCreate()
sc = uploading_session.sparkContext

# Hadoop Configuration

hadoop_conf = sc._jsc.hadoopConfiguration()
access_key = config.a_k
secret_key = config.s_k
endpoint = "https://s3.us-west-1.amazonaws.com"

hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.endpoint", endpoint)


# Read Parquet file into DataFrame and write it as Parquet file into S3 bucket
Fact_Sales = uploading_session.read.parquet("/usr/local/spark/resources/output/Star_Schema_Tables/Fact_Sales")
Fact_Sales.write.mode('overwrite').parquet("s3a://aaa-raw-data/Star_Schema_Tables/Fact_Sales")
rows = Fact_Sales.count()
logger.info(f"Parquet file 'Fact_Sales' was successfully loaded into DataFrame and uploaded as Parquet file into S3 bucket. {rows} rows loaded")

# Record script running time
script_time = round(time.time() - star_time, 2)
logger.info(f"'daily_data_uploading' script was successfully executed. Runnig time was {script_time} secs")