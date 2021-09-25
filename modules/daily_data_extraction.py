from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
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
logger = logging.getLogger("daily_data_extraction")
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
        table (str): Table's name.
    
    Returns:
        DataFrame
    '''
    
    dataframe = extraction_session.read.format("jdbc")\
                    .option("url", url)\
                    .option("driver", driver)\
                    .option("dbtable", table)\
                    .option("user", u)\
                    .option("password", p).load()

    return dataframe


def loaded_df_check (df, table_name):
    '''Confirm table was read successfully into DataFrame.
            
    Args:
        df (dataframe): DataFrame to check.
        table_name (str): Source table name.
    '''
    if df.rdd.isEmpty():
        logger.error(f"Table '{table_name}' is empty.")
    else:
        rows = df.count()
        logger.info(f"Table '{table_name}' was successfully loaded. {rows} rows loaded.")


def schema_check (df, table_name, fields):
    '''Confirm DataFrame was reduced successfully to required fields.
            
    Args:
        df (dataframe): DataFrame to check.
        table_name (str): Source table name.
        fields (int): Field count.
    '''

    if len(df.columns) == fields:
        logger.info(f"Table '{table_name}' was successfully reduced to required fields.")
    else:
        logger.critical(f"Table '{table_name}' was not reduced to required fields. Program aborted.")
        sys.exit()


# Start SparkSession (entry point to Spark)
extraction_session = SparkSession.builder.master("spark://spark:7077").appName("Daily_Data_Extraction").getOrCreate()


# MySQL database configuration values
url = config.db_url
driver = "com.mysql.cj.jdbc.Driver"
u = config.db_u
p = config.db_p


# Read MySQL tables into DataFrames
so = table_to_dataframe("so")
loaded_df_check (so, "so")
soitem = table_to_dataframe("soitem")
loaded_df_check (soitem, "soitem")


# Extract daily transactions at order level. Reduce "so" table to required columns only.
today_dt = dt.date.today()
yesterday_dt = today_dt - dt.timedelta(days=1) 
yesterday = yesterday_dt.strftime("%Y-%m-%d")

reduced_so = so.select("id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId")\
                .filter(F.col("dateCreated").between(f"{yesterday} 00:00:00", f"{yesterday} 23:59:59"))
schema_check(reduced_so, "so", 9)

# Extract transactions at item level. Reduce "soitem" table to required columns only.
reduced_soitem = soitem.select("id", "productId", "qtyOrdered", "soId", "typeId")
schema_check(reduced_soitem, "soitem", 5)


# Save DataFrames (locally) into Parquet and CSV files
reduced_so.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_so")
reduced_so.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_so.csv")
logger.info(f"DataFrame 'reduced_so' was successfully saved as Parquet and CSV file")
reduced_soitem.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_soitem")
reduced_soitem.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/daily_transactions/r_soitem.csv")
logger.info(f"DataFrame 'reduced_soitem' was successfully saved as Parquet and CSV file")


# Record script running time
script_time = round(time.time() - star_time, 2)
logger.info(f"'daily_data_extraction' script was successfully executed. Runnig time was {script_time} secs")