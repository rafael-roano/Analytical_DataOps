import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType
import logging
import sys
import time


# Start timer to record script running time
star_time = time.time()

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

console_handler = logging.StreamHandler()
console_handler.addFilter(HandlerFilter(logging.INFO))
logger.addHandler(console_handler)

file_handler = logging.FileHandler("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Logs\\pipeline.log")
file_handler.setFormatter(formatter)
# file_handler.addFilter(HandlerFilter(logging.WARNING))
logger.addHandler(file_handler)


sql_session = SparkSession.builder.appName('FBL_Extraction').getOrCreate()
url = "jdbc:mysql://localhost:3305/3a17"
driver = "com.mysql.cj.jdbc.Driver"
u = ""
p = ""
tables = ["so", "soitem", "product", "part"]

def table_to_dataframe (table):
    '''Read table from SQL database and returns a DataFrame.
            
    Args:
        table (str): Table's name.
    
    Returns:
        DataFrame
    '''
    
    dataframe = sql_session.read.format("jdbc")\
                    .option("url", url)\
                    .option("driver", driver)\
                    .option("dbtable", table)\
                    .option("user", u)\
                    .option("password", p).load() 

    return dataframe

def table_read_check (df, table_name):
    '''Confirm table was read successfully into DataFrame.
            
    Args:
        df (dataframe): DataFrame to check.
        table_name (str): Source table name.
    '''
    if df.rdd.isEmpty():
        logger.error(f"Table {table_name} is empty")
    else:
        rows = df.count()
        logger.info(f"Table {table_name} was successfully loaded. {rows} rows loaded")

def schema_check (df, table_name, fields):
    '''Confirm DataFrame was reduced successfully to required fields.
            
    Args:
        df (dataframe): DataFrame to check.
        table_name (str): Source table name.
        fields (int): Field count.
    '''

    if len(df.columns) == fields:
        logger.info(f"Table {table_name} was successfully reduced to required fields")
    else:
        logger.critical(f"Table {table_name} was not reduced to required fields. Program aborted")
        sys.exit()

def mask_part(num):
    '''Mask part code based on masking dictionary.
            
    Args:
        num (str): Part code.
    
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

udf_mask_part = F.udf(mask_part, StringType())

def masking_check (df, df_name, fields):
    '''
    Confirm DataFrame was masked successfully.
            
    Args:
        df (dataframe): DataFrame to check.
        df_name (str): DataFrame name.
        fields (int): Field count.
    '''

    unmasked = masked_part.filter(masked_part.masked_num.contains('.')).collect()
    
    if (len(df.columns) == fields) and (len(unmasked) == 0):
        logger.info(f"DataFrame {df_name} was successfully masked")
    else:
        logger.critical(f"DataFrame {df_name} was not successfully masked. Program aborted")
        sys.exit()


# Read SQL tables into DataFrames
so = table_to_dataframe("so")
table_read_check (so, "so")
soitem = table_to_dataframe("soitem")
table_read_check (soitem, "soitem")
product = table_to_dataframe("product")
table_read_check (product, "product")
part = table_to_dataframe("part")
table_read_check (part, "part")


# Select only columns that add value to the project. For "so" table, selecting only data from 2021.
reduced_so = so.select("id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId").filter(F.col("dateCreated").between("2021-01-01 00:00:00", "2021-12-31 23:59:59"))
schema_check(reduced_so, "so", 9)
reduced_soitem = soitem.select("id", "productId", "qtyOrdered", "soId", "typeId")
schema_check(reduced_soitem, "soitem", 5)
reduced_product = product.select("id", "partId")
schema_check(reduced_product, "product", 2)
reduced_part = part.select("id", "height", "len", "num", "typeId", "width", "customFields")
schema_check(reduced_part, "part", 7)


# Mask column of part codes on reduced_part table based on dictionary with masking values (from csv source)
mask = sql_session.read.option("header", True).csv("C:\\Users\\FBLServer\\Documents\\Jupyter\\mask.csv")
mask_list = mask.collect()
mask_dict = {}

for row in mask_list:
    mask_dict[row[0]] = row[1]

masked_part = reduced_part.withColumn("masked_num", udf_mask_part(reduced_part.num)).drop(reduced_part.num)
masking_check(masked_part, "reduced_part", 7)

# Save DataFrames (locally) into parquet files
reduced_so.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\r_so.parquet")
logger.info(f"DataFrame reduced_so was successfully saved as parquet file")
reduced_soitem.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\r_soitem.parquet")
logger.info(f"DataFrame reduced_soitem was successfully saved as parquet file")
reduced_product.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\r_product.parquet")
logger.info(f"DataFrame reduced_product was successfully saved as parquet file")
masked_part.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\m_part.parquet")
logger.info(f"DataFrame masked_part was successfully saved as parquet file")



cleaning_session = SparkSession.builder.appName('Data_Cleaning').getOrCreate()

# Read Partquet files into DataFrames
so = cleaning_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\r_so.parquet")
rows = so.count()
logger.info(f"Parquet file so was successfully loaded into DataFrame. {rows} rows loaded")
soitem = cleaning_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\r_soitem.parquet")
rows = soitem.count()
logger.info(f"Parquet file soitem was successfully loaded into DataFrame. {rows} rows loaded")
product = cleaning_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\r_product.parquet")
rows = product.count()
logger.info(f"Parquet file product was successfully loaded into DataFrame. {rows} rows loaded")
part = cleaning_session.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\m_part.parquet")
rows = part.count()
logger.info(f"Parquet file part was successfully loaded into DataFrame. {rows} rows loaded")

# so DataFrame transformations

# Filter out orders that have order status not required (80: Voided, 85: Cancelled, 90: Expired, 95: Historical).
so = so.filter((so.statusId != 80) & (so.statusId != 85) & (so.statusId != 90) & (so.statusId != 95))
logger.info(f"Trans. on DataFrame so completed: Transactions with status 80, 85, 90 or 95 were filtered out")
rows = so.count()
logger.info(f"Rows count is {rows}")
so.show()  

# Replace null value in "currencyId" field by 1, which equals USD
so = so.na.fill({'currencyId': 1})
logger.info(f"Trans. on DataFrame so completed: Currency field with value null was replaced with 1 (USD)")
rows = so.count()
logger.info(f"Rows count is {rows}")
so.show()

# Categorize transactions by Sales Channel
so = so.select(so['*'], 
                                     F.when((so.num.like("%#SS%")) | (so.num.like("%#CS%")) | (so.num.like("%#MS%")) | (so.num.like("%Samples%")) | (so.num.like("%SAMPLES%")) | (so.num.like("%samples%")), "Samples")
                                     .when(so.num.like("%RMA%"), "RMA")
                                     .when(so.customerId == 3738, "C")
                                     .when(so.customerId == 1731, "G")
                                     .when(so.customerId == 1, "H")
                                     .when(so.customerId == 6342, "J")
                                     .when(so.customerId == 3806, "N")
                                     .when(so.customerId == 4854, "A")
                                     .when(so.customerId == 3995, "B")
                                     .when(so.customerId == 426, "E")
                                     .when(so.customerId == 312, "F")
                                     .when(so.customerId == 2839, "I")
                                     .when(so.customerId == 3809, "M")
                                     .when(so.customerId == 6343, "Q")
                                     .when(so.customerId == 3188, "R")
                                     .when((so.customerId == 3852) | (so.customerId == 1159), "Others")
                                     .when((so.qbClassId == 19) & (so.customerId != 3738) & (so.customerId != 3806), "K")
                                     .when((so.qbClassId == 9) & (so.customerId != 312) & (so.customerId != 3809) & (so.customerId != 6342), "O")
                                     .when((so.qbClassId == 12) | (so.qbClassId == 17), "P")                                     
                                     .otherwise("Others").alias("sales_channel"))
logger.info(f"Trans. on DataFrame so completed: Sales transactions were categorized")
rows = so.count()
logger.info(f"Rows count is {rows}")
so.show()

# Remove categories not required (Others, Samples, RMA)
so = so.select("id", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId", "sales_channel").filter(so.sales_channel != 'Others').filter(so.sales_channel != 'Samples').filter(so.sales_channel != 'RMA')
logger.info(f"Trans. on DataFrame so completed: Sales transactions with categories Others, Samples and RMA were filtered out")
rows = so.count()
logger.info(f"Rows count is {rows}")
so.show()

# soitem DataFrame transformations

# Filter only items with typeId = 10 (Sale Items)
soitem = soitem.filter(soitem.typeId == 10)
logger.info(f"Trans. on DataFrame soitem completed: Items with typeId 10 were filtered")
rows = soitem.count()
logger.info(f"Rows count is {rows}")
soitem.show()

# Round qtyOrdered field to zero digits
soitem = soitem.withColumn("qtyOrdered_r", F.round(soitem.qtyOrdered, 0)).drop(soitem.qtyOrdered)
logger.info(f"Trans. on DataFrame soitem completed: Column qtyOrdered was rounded to zero digits")
rows = soitem.count()
logger.info(f"Rows count is {rows}")
soitem.show()

# part DataFrame transformations

# Round fileds len, width and height to 2 digits
part = part.withColumn("len_r", F.round(part.len, 2)).drop(part.len)
part = part.withColumn("width_r", F.round(part.width, 2)).drop(part.width)
part = part.withColumn("height_r", F.round(part.height, 2)).drop(part.height)
logger.info(f"Trans. on DataFrame part completed: Columns len, width and height were rounded to 2 digits")
rows = part.count()
logger.info(f"Rows count is {rows}")

# Joins

# Join DataFrame soitem with DataFrame so
categorized_items = soitem.join(so, soitem.soId ==  so.id)
categorized_items.select(soitem.soId, soitem.productId, soitem.qtyOrdered_r, so.sales_channel).show()
logger.info(f"Trans: DataFrame soitem joined with DataFrame so")
rows = categorized_items.count()
logger.info(f"Rows count is {rows}")

# Join DataFrame categorized_items with DataFrame product
categorized_items = categorized_items.join(product, categorized_items.productId == product.id)
categorized_items.select(categorized_items.soId, categorized_items.productId, categorized_items.qtyOrdered_r, categorized_items.sales_channel,product.partId).show()
logger.info(f"Trans: DataFrame categorized_items joined with DataFrame product")
rows = categorized_items.count()
logger.info(f"Rows count is {rows}")

# Join DataFrame categorized_items with DataFrame part
categorized_items = categorized_items.join(part, categorized_items.partId == part.id)
categorized_items.select(categorized_items.soId, categorized_items.productId, categorized_items.qtyOrdered_r, categorized_items.sales_channel, categorized_items.partId, part.masked_num).show()
logger.info(f"Trans: DataFrame categorized_items joined with DataFrame part")
rows = categorized_items.count()
logger.info(f"Rows count is {rows}")

categorized_items.printSchema()

# Create Fact_Sales table
Fact_Sales = categorized_items.select(categorized_items.dateCreated, categorized_items.sales_channel, categorized_items.masked_num, categorized_items.qtyOrdered_r)
Fact_Sales.show()
Fact_Sales.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Fact_Sales.parquet")
logger.info(f"Table Fact_Sales was successfully saved as parquet file")

# Create Dim_Products table
Dim_Products = part.select("id", "masked_num", "len_r", "width_r", "height_r")
Dim_Products.show()
Dim_Products.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Dim_Products.parquet")
logger.info(f"Table Dim_Products was successfully saved as parquet file")

# Create Dim_Sales_Channels table
sales_cat_schema = StructType([
	StructField("id", StringType(), False),
    StructField("platform_type", StringType(), False),
    StructField("B2", StringType(), False),
    StructField("drop_shipping", BooleanType(), False),
    
])

Dim_Sales_Channels = cleaning_session.read.option("header", True).schema(sales_cat_schema).csv("C:\\Users\\FBLServer\\Documents\\Jupyter\\sales_cat.csv")
Dim_Sales_Channels.printSchema()
Dim_Sales_Channels.show()
Dim_Sales_Channels.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Dim_Sales_Channels.parquet")
logger.info(f"Table Dim_Sales_Channels was successfully saved as parquet file")

# Create Dim_Dates table
date_schema = StructType([
	StructField("date", DateType(), False),    
])

Dim_Dates = cleaning_session.read.option("header", True).schema(date_schema).csv("C:\\Users\\FBLServer\\Documents\\Jupyter\\date.csv")
Dim_Dates.printSchema()
Dim_Dates.show()
Dim_Dates.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Dim_Dates.parquet")
logger.info(f"Table Dim_Dates was successfully saved as parquet file")

script_time = round(time.time() - star_time, 2)
logger.info(f"Script runnig time was {script_time} secs")