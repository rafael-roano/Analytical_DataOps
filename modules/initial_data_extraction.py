from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging
import time
import sys

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
logger = logging.getLogger("intial_data_extraction")
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


def part_masking(num):
    '''Mask part num based on masking dictionary.
            
    Args:
        num (str): Original part num.
    
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


def masking_check (df, df_name, fields):
    '''
    Confirm DataFrame was masked successfully.
            
    Args:
        df (dataframe): DataFrame to check.
        df_name (str): DataFrame name.
        fields (int): Field count.
    '''

    # Filter column masked_num to check values with "."; should equal zero
    unmasked = masked_part.filter(masked_part.masked_num.contains('.')).collect()
    
    if (len(df.columns) == fields) and (len(unmasked) == 0):
        logger.info(f"DataFrame '{df_name}' was successfully masked.")
    else:
        logger.critical(f"DataFrame '{df_name}' was not successfully masked. Program aborted.")
        sys.exit()

    # if len(df.columns) == fields:
    #     logger.info(f"DataFrame {df_name} was successfully masked.")
    # else:
    #     logger.critical(f"DataFrame {df_name} was not successfully masked. Program aborted.")
    #     sys.exit()



# Start SparkSession (entry point to Spark)
extraction_session = SparkSession.builder.master("spark://spark:7077").appName("Initial_Data_Extraction").getOrCreate()


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
product = table_to_dataframe("product")
loaded_df_check (product, "product")
part = table_to_dataframe("part")
loaded_df_check (part, "part")


# Extract transactions at order level for specific time period (initial extraction). Reduce tables to required columns only.

reduced_so = so.select("id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId")\
                .filter(F.col("dateCreated").between("2021-01-01 00:00:00", "2021-09-11 23:59:59"))
schema_check(reduced_so, "so", 9)
reduced_soitem = soitem.select("id", "productId", "qtyOrdered", "soId", "typeId")
schema_check(reduced_soitem, "soitem", 5)
reduced_product = product.select("id", "partId")
schema_check(reduced_product, "product", 2)
reduced_part = part.select("id", "height", "len", "num", "typeId", "width", "customFields")
schema_check(reduced_part, "part", 7)


# Mask "part num" column in reduced_part table based on dictionary with masking values (mask by substitution).

# CSV file from local machine loaded to DataFrame -> DataFrame collected into an array
mask = extraction_session.read.option("header", True).csv("/usr/local/spark/resources/x/m.csv")
mask_array = mask.collect()
mask_dict = {}

# Iterate over array to populate dictionary with substitution values. Key = Original value, Value = Masking value
for row in mask_array:
    mask_dict[row[0]] = row[1]


# Create DataFrame with masked part num column (using UDF) and droping original part num column
udf_part_masking = F.udf(part_masking, StringType())
masked_part = reduced_part.withColumn("masked_num", udf_part_masking(reduced_part.num)).drop(reduced_part.num)
masking_check(masked_part, "reduced_part", 7)


# Save DataFrames (locally) into Parquet and CSV files
reduced_so.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/r_so")
reduced_so.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/r_so.csv")
logger.info(f"DataFrame 'reduced_so' was successfully saved as Parquet and CSV file")
reduced_soitem.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/r_soitem")
reduced_soitem.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/r_soitem.csv")
logger.info(f"DataFrame 'reduced_soitem' was successfully saved as Parquet and CSV file")
reduced_product.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/r_product")
reduced_product.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/r_product.csv")
logger.info(f"DataFrame 'reduced_product' was successfully saved as Parquet and CSV file")
masked_part.write.mode('overwrite').parquet("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/m_part")
masked_part.coalesce(1).write.mode('overwrite').csv("/usr/local/spark/resources/output/Extracted_MySQL_Tables/initial_extraction/m_part.csv")
logger.info(f"DataFrame 'masked_part' was successfully saved as Parquet and CSV file")


# Record script running time
script_time = round(time.time() - star_time, 2)
logger.info(f"'initial_data_extraction' script was successfully executed. Runnig time was {script_time} secs")