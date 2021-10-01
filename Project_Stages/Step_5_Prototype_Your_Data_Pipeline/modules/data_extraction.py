# Prototype version of data_extraction module

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging
import sys


sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
import config

# import time
# Start timer to record script running time
# star_time = time.time()


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
logger = logging.getLogger("data_extraction")
logger.setLevel(logging.INFO)

# Handler setup (send the log records to the appropriate destination)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

file_handler = logging.FileHandler("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\OECP\\Project_Stages\\Step_5_Prototype_Your_Data_Pipeline\\data\\pipeline.log")
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


def reduce_so_table (df, fields, field_count, start_date, finish_date):
    '''Select only required fields and 'Date Created' range from 'so' table.
            
    Args:
        df (dataframe): DataFrame to reduce
        fields (list): Fields to keep from table
        field_count (int): Field count to be reduced to
        start_date (str): Time window start date
        finish_date (str): Time window finisn date
    '''

    reduced_so = df.select(fields).filter(F.col("dateCreated").between(start_date, finish_date))

    if len(reduced_so.columns) == field_count:
        rows = reduced_so.count()
        logger.info(f"Table 'so' was successfully reduced to required fields: {field_count}. Row count is {rows}.")
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


@F.udf(returnType=StringType())
def mask_part(num):
    '''Mask part num based on masking dictionary.
            
    Args:
        num (str): Original part number
    
    Returns:
        str
    '''
    masked_chars = []
    masked = ""
    
    for char in num:
        
        if char in mask_dict.keys():
            masked_char = mask_dict.get(char)
            masked_chars.append(masked_char)
        else:
            masked_chars.append(char)
    
    return masked.join(masked_chars)


def check_masking (df, field_count):
    '''
    Confirm DataFrame was masked successfully.
            
    Args:
        df (dataframe): DataFrame to check
        field_count (int): Field count
    '''

    # Filter column masked_num to check values with "."; should equal zero
    unmasked = df.filter(df.masked_num.contains('.')).collect()
    
    if (len(df.columns) == field_count) and (len(unmasked) == 0):
        logger.info(f"DataFrame 'reduced_part' was successfully masked.")
    else:
        logger.critical(f"DataFrame 'reduced_part' was not successfully masked. Program aborted.")
        sys.exit()



# Start SparkSession (entry point to Spark)
extraction_session = SparkSession.builder.master("local[*]").appName("Data_Extraction").getOrCreate()


# MySQL database configuration values
url = config.db_url
driver = "com.mysql.cj.jdbc.Driver"
u = config.db_u
p = config.db_p


# Read MySQL tables into DataFrames
so = table_to_dataframe("so")
soitem = table_to_dataframe("soitem")
product = table_to_dataframe("product")
part = table_to_dataframe("part")


# Reduce 'so' table to required fields and extract transactions for specific time period (initial extraction).
so_req_fields = ["id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId"]
start_date = "2021-01-01 00:00:00"
finish_date = "2021-12-31 23:59:59"
reduced_so = reduce_so_table(so, so_req_fields, 9, start_date, finish_date)


# Reduce tables to required fields only.
soitem_req_fields = ["id", "productId", "qtyOrdered", "soId", "typeId"]
reduced_soitem = reduce_table(soitem, soitem_req_fields, 5, "soitem")
product_req_fields = ["id", "partId"]
reduced_product = reduce_table(product, product_req_fields, 2, "product")
part_req_fields = ["id", "height", "len", "num", "typeId", "width", "customFields"]
part_field_count = 7
reduced_part = reduce_table(part, part_req_fields, part_field_count, "part")


## Mask part num column in reduced_part table based on dictionary with masking values. Mask by substitution.

# CSV file from local machine loaded to DataFrame -> DataFrame collected into an array
mask = extraction_session.read.option("header", True).csv("C:\\Users\\FBLServer\\Documents\\c\\m.csv")
mask_array = mask.collect()
mask_dict = {}

# Iterate over array to populate dictionary with substitution values. Key = Original value, Value = Masking value
for row in mask_array:
    mask_dict[row[0]] = row[1]

# Create DataFrame with masked part num column (using UDF) and droping original part num column
masked_part = reduced_part.withColumn("masked_num", mask_part(reduced_part.num)).drop(reduced_part.num)
check_masking(masked_part, part_field_count)


# Save DataFrames (locally) into parquet files
reduced_so.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\r_so")
reduced_so.write.mode('overwrite').csv("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\r_so.csv")
logger.info(f"DataFrame 'reduced_so' was successfully saved as parquet file")
reduced_soitem.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\r_soitem")
reduced_soitem.write.mode('overwrite').csv("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\r_soitem.csv")
logger.info(f"DataFrame 'reduced_soitem' was successfully saved as parquet file")
reduced_product.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\r_product")
reduced_product.write.mode('overwrite').csv("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\r_product.csv")
logger.info(f"DataFrame 'reduced_product' was successfully saved as parquet file")
masked_part.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\m_part")
masked_part.write.mode('overwrite').csv("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\m_part.csv")
logger.info(f"DataFrame 'masked_part' was successfully saved as parquet file")


# Record script running time
# script_time = round(time.time() - star_time, 2)
# logger.info(f"Script runnig time was {script_time} secs")