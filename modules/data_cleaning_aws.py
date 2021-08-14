from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
import logging

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
logger = logging.getLogger("data_cleaning")
logger.setLevel(logging.INFO)

# Handler setup (send the log records to the appropriate destination)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

file_handler = logging.FileHandler("pipeline.log")
logger.addHandler(file_handler)

# Filter setup (based on the message level)
console_handler.addFilter(HandlerFilter(logging.INFO))
# file_handler.addFilter(HandlerFilter(logging.WARNING))

# Formatter setup (specify the layout of log records in the final output)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
file_handler.setFormatter(formatter)


# Start SparkSession (entry point to Spark)
cleaning_session = SparkSession.builder.appName('Data_Cleaning').getOrCreate()
sc = cleaning_session.sparkContext
sc.setLogLevel("ERROR")

# Read Parquet files into DataFrames
so = cleaning_session.read.parquet("s3a://aaa-raw-data/mysql_extracted_data/r_so")
rows = so.count()
logger.info(f"Parquet file 'r_so' was successfully loaded into DataFrame. {rows} rows loaded")
soitem = cleaning_session.read.parquet("s3a://aaa-raw-data/mysql_extracted_data/r_soitem")
rows = soitem.count()
logger.info(f"Parquet file 'r_soitem' was successfully loaded into DataFrame. {rows} rows loaded")
product = cleaning_session.read.parquet("s3a://aaa-raw-data/mysql_extracted_data/r_product")
rows = product.count()
logger.info(f"Parquet file 'r_product' was successfully loaded into DataFrame. {rows} rows loaded")
part = cleaning_session.read.parquet("s3a://aaa-raw-data/mysql_extracted_data/m_part")
rows = part.count()
logger.info(f"Parquet file 'm_part' was successfully loaded into DataFrame. {rows} rows loaded")

# 'so' DataFrame transformations

# Filter out orders that have order status not required (80: Voided, 85: Cancelled, 90: Expired, 95: Historical)
so = so.filter((so.statusId != 80) & (so.statusId != 85) & (so.statusId != 90) & (so.statusId != 95))
logger.info(f"Transformation on DataFrame 'so' completed: Transactions with status 80, 85, 90 or 95 were filtered out")
rows = so.count()
logger.info(f"Row count is {rows}")

# Replace null value in "currencyId" field with 1, which equals USD
so = so.na.fill({'currencyId': 1})
logger.info(f"Transformation on DataFrame 'so' completed: Currency field with value null was replaced with 1 (USD)")
rows = so.count()
logger.info(f"Row count is {rows}")

# Categorize transactions by Sales Channel
so = so.select(so['*'], 
                                     F.when((so.num.like("%#SS%")) | (so.num.like("%#CS%")) | (so.num.like("%#MS%"))\
                                            | (so.num.like("%Samples%")) | (so.num.like("%SAMPLES%")) | (so.num.like("%samples%")), "Samples")
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
                                     .when((so.customerId == 3852) | (so.customerId == 1159), "Closed Channel")
                                     .when((so.qbClassId == 19) & (so.customerId != 3738) & (so.customerId != 3806), "K")
                                     .when((so.qbClassId == 9) & (so.customerId != 312) & (so.customerId != 3809) & (so.customerId != 6342), "O")
                                     .when((so.qbClassId == 12) | (so.qbClassId == 17), "P")                                     
                                     .otherwise("Uncategorized").alias("sales_channel"))
logger.info(f"Transformation on DataFrame 'so' completed: Sales transactions were categorized")
rows = so.count()
logger.info(f"Row count is {rows}")

# Save transactions not required (Samples, RMA, Closed Channel, Uncategorized) in CSV file as backup

not_req = so.filter(so.sales_channel.isin(["Samples", "RMA", "Closed Channel", "Uncategorized"]))
logger.info(f"Transactions not required in Fact_Sales table were loaded into DataFrame 'not_req'")
rows =not_req.count()
logger.info(f"Row count is {rows}")
not_req.write.mode('overwrite').csv("s3a://aaa-raw-data/mysql_extracted_data/not_required_trans")

# Remove transactions not required from "so" DataFrame (Samples, RMA, Closed Channel, Uncategorized)
so = so.select("id", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId", "sales_channel")\
                .filter(~so.sales_channel.isin(["Samples", "RMA", "Closed Channel", "Uncategorized"]))
logger.info(f"Transformation on DataFrame 'so' completed: Sales transactions with categories: Samples, RMA, Closed Channel or Uncategorized were filtered out")
rows = so.count()
logger.info(f"Row count is {rows}")


# 'soitem' DataFrame transformations

# Filter only items with typeId = 10 (Sale Items)
soitem = soitem.filter(soitem.typeId == 10)
logger.info(f"Transformation on DataFrame 'soitem' completed: Items with typeId 10 were filtered")
rows = soitem.count()
logger.info(f"Row count is {rows}")

# Round qtyOrdered field to zero digits
soitem = soitem.withColumn("qtyOrdered_r", F.round(soitem.qtyOrdered, 0)).drop(soitem.qtyOrdered)
logger.info(f"Transformation on DataFrame 'soitem' completed: Column qtyOrdered was rounded to zero digits")
rows = soitem.count()
logger.info(f"Row count is {rows}")

# 'part' DataFrame transformations

# Round fields: len, width and height to 2 digits
part = part.withColumn("len_r", F.round(part.len, 2)).drop(part.len)
part = part.withColumn("width_r", F.round(part.width, 2)).drop(part.width)
part = part.withColumn("height_r", F.round(part.height, 2)).drop(part.height)
logger.info(f"Transformation on DataFrame 'part' completed: Columns: len, width and height were rounded to 2 digits")
rows = part.count()
logger.info(f"Row count is {rows}")

# Joins

# Join DataFrame 'soitem' with DataFrame 'so'
categorized_items = soitem.join(so, soitem.soId ==  so.id)
# categorized_items.select(soitem.productId, soitem.qtyOrdered_r, so.sales_channel).show()
logger.info(f"Join completed. DataFrame 'soitem' joined with DataFrame 'so'")
rows = categorized_items.count()
logger.info(f"Row count is {rows}")

# Join DataFrame 'categorized_items' with DataFrame 'product'
categorized_items = categorized_items.join(product, categorized_items.productId == product.id)
# categorized_items.select(product.partId, categorized_items.qtyOrdered_r, categorized_items.sales_channel).show()
logger.info(f"Join completed. DataFrame 'categorized_items' joined with DataFrame 'product'")
rows = categorized_items.count()
logger.info(f"Row count is {rows}")

# Join DataFrame 'categorized_items' with DataFrame 'part'
categorized_items = categorized_items.join(part, categorized_items.partId == part.id)
# categorized_items.select(part.masked_num, categorized_items.qtyOrdered_r, categorized_items.sales_channel).show()
logger.info(f"Join completed. DataFrame 'categorized_items' joined with DataFrame 'part'")
rows = categorized_items.count()
logger.info(f"Row count is {rows}")

categorized_items.printSchema()

# Create 'Fact_Sales' table
Fact_Sales = categorized_items.select(categorized_items.dateCreated.alias("Date_Id"), categorized_items.sales_channel.alias("Sales_Channel_Id"), \
                        categorized_items.masked_num.alias("Product_Id"), categorized_items.qtyOrdered_r.alias("Units_Sold"))
Fact_Sales.write.mode('overwrite').parquet("s3a://aaa-raw-data/Star_Schema_Tables/Fact_Sales")
logger.info(f"Table 'Fact_Sales' was successfully saved as Parquet file")

# Create 'Dim_Products' table
Dim_Products = part.select("id", "masked_num", "len_r", "width_r", "height_r")
Dim_Products.write.mode('overwrite').parquet("s3a://aaa-raw-data/Star_Schema_Tables/Dim_Products")
logger.info(f"Table Dim_Products was successfully saved as Parquet file")

# Create 'Dim_Sales_Channels' table
sales_cat_schema = StructType([
	StructField("id", StringType(), False),
    StructField("platform_type", StringType(), False),
    StructField("B2", StringType(), False),
    StructField("drop_shipping", BooleanType(), False),
    
])

Dim_Sales_Channels = cleaning_session.read.option("header", True).schema(sales_cat_schema).csv("s3a://aaa-raw-data/other_tables/sales_cat.csv")
Dim_Sales_Channels.printSchema()
Dim_Sales_Channels.write.mode('overwrite').parquet("s3a://aaa-raw-data/Star_Schema_Tables/Dim_Sales_Channels")
logger.info(f"Table 'Dim_Sales_Channels' was successfully saved as Parquet file")

# Create 'Dim_Dates' table
date_schema = StructType([
	StructField("date", TimestampType(), False),
    StructField("day", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("year", IntegerType(), False),    
])

Dim_Dates = cleaning_session.read.option("header", True).schema(date_schema).csv("s3a://aaa-raw-data/other_tables/date.csv")
Dim_Dates.printSchema()
Dim_Dates.write.mode('overwrite').parquet("s3a://aaa-raw-data/Star_Schema_Tables/Dim_Dates")
logger.info(f"Table 'Dim_Dates' was successfully saved as Parquet file")

# Record script running time
# script_time = round(time.time() - star_time, 2)
# logger.info(f"Script runnig time was {script_time} secs")