from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import time
import sys
import datetime as dt

sys.path.append("/usr/local/spark/resources/x/")
import config


# Start timer to record script running time
star_time = time.time()

# Logging setup
class HandlerFilter():
    '''Class to filter handler based on message levels.'''
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
logger = logging.getLogger("daily_data_extraction_aws")
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


def table_to_dataframe (table):
    '''Read table from MySQL database and returns a DataFrame.
            
    Args:
        table (str): Table's name
    
    Returns:
        DataFrame
    '''
    try:

        df = extraction_session.read.format("jdbc")\
                        .option("url", url)\
                        .option("driver", driver)\
                        .option("dbtable", table)\
                        .option("user", u)\
                        .option("password", p).load()
    
    except:
        logger.critical(f"Unexpected error during '{table}' table reading from database. Unexpected error: {sys.exc_info()}. Progam aborted.")
        sys.exit()
          
    else:
        rows = df.count()
        logger.info(f"Table '{table}' was successfully loaded. {rows} rows loaded.")
    
    return df


def reduce_so_table (df, fields, field_count, yesterday):
    '''Select only required fields and yesterday's date as'Date Created' range from 'so' table.
            
    Args:
        df (dataframe): DataFrame to reduce
        fields (list): Fields to keep from table
        field_count (int): Field count to be reduced to
        yesterday (str): Yesterday's date
    '''

    reduced_so = df.select(fields).filter(F.col("dateCreated").between(f"{yesterday} 00:00:00", f"{yesterday} 23:59:59"))
   
    if len(reduced_so.columns) == field_count:
        rows = reduced_so.count()
        logger.info(f"Table 'so' was successfully reduced to required fields: {field_count}. Transactions from {yesterday} were filtered. Row count is {rows}.")
        return reduced_so
    else:
        logger.critical(f"Table 'so' was not reduced to required fields. Program aborted.")
        sys.exit()



def reduce_table (df, fields, field_count, table_name):
    '''Select only required fields from table.
            
    Args:
        df (dataframe): DataFrame to reduce
        fields (list): Fields to keep from table
        field_count (int): Field count to be reduced to
        table_name (str): Source table name
    '''
   
    reduced_df = df.select(fields)

    if len(reduced_df.columns) == field_count:
        rows = reduced_df.count()
        logger.info(f"Table '{table_name}' was successfully reduced to required fields: {field_count}. Row count is {rows}.")
        return reduced_df
    else:
        logger.critical(f"Table '{table_name}' was not reduced to required fields. Program aborted.")
        sys.exit()


# Start SparkSession (entry point to Spark)
extraction_session = SparkSession.builder.master("spark://spark:7077").appName('daily_data_extraction_aws').getOrCreate()
sc = extraction_session.sparkContext

# Hadoop Configuration
hadoop_conf = sc._jsc.hadoopConfiguration()
access_key = config.a_k
secret_key = config.s_k
endpoint = "https://s3.us-west-1.amazonaws.com"
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.endpoint", endpoint)


# MySQL database configuration values
url = config.db_url
driver = "com.mysql.cj.jdbc.Driver"
u = config.db_u
p = config.db_p


# Read MySQL tables into DataFrames
so = table_to_dataframe("so")
soitem = table_to_dataframe("soitem")


# Extract daily transactions at order level. Reduce "so" table to required columns only.
today_dt = dt.date.today()
yesterday_dt = today_dt - dt.timedelta(days=1) 
yesterday = yesterday_dt.strftime("%Y-%m-%d")
so_req_fields = ["id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId"]
reduced_so = reduce_so_table(so, so_req_fields, 9, yesterday)


# Extract transactions at item level from "soitem" table and reduce it to required columns only.
soitem_req_fields = ["id", "productId", "qtyOrdered", "soId", "typeId"]
reduced_soitem = reduce_table(soitem, soitem_req_fields, 5, "soitem")


# Save DataFrames (locally) into Parquet and CSV files
reduced_so.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_so")
reduced_so.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_so.csv")
logger.info(f"DataFrame 'reduced_so' was successfully saved as Parquet and CSV file")
reduced_soitem.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_soitem")
reduced_soitem.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_soitem.csv")
logger.info(f"DataFrame 'reduced_soitem' was successfully saved as Parquet and CSV file")


# Write Parquet files into S3 bucket
reduced_so.write.mode('overwrite').parquet("s3a://aaa-raw-data/Extracted_MySQL_Tables/daily_transactions/r_so")
logger.info(f"DataFrame 'reduced_so' was successfully saved as Parquet file on S3 bucket")
reduced_soitem.write.mode('overwrite').parquet("s3a://aaa-raw-data/Extracted_MySQL_Tables/daily_transactions/r_soitem")
logger.info(f"DataFrame 'reduced_soitem' was successfully saved as Parquet file on S3 bucket")



# Record script running time
script_time = round(time.time() - star_time, 2)
logger.info(f"'daily_data_extraction_aws' script was successfully executed. Runnig time was {script_time} secs")