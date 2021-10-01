# Prototype version of data_cleaning module

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
import logging
import sys

sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
import config as c

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

file_handler = logging.FileHandler("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\OECP\\Project_Stages\\Step_5_Prototype_Your_Data_Pipeline\\data\\pipeline.log")
logger.addHandler(file_handler)

# Filter setup (based on the message level)
console_handler.addFilter(HandlerFilter(logging.INFO))
# file_handler.addFilter(HandlerFilter(logging.WARNING))

# Formatter setup (specify the layout of log records in the final output)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
file_handler.setFormatter(formatter)


def read_parquet(file):

    df = cleaning_session.read.parquet(f"C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Extracted_MySQL_Tables\\{file}")
    rows = df.count()
    logger.info(f"Parquet file '{file}' was successfully loaded into DataFrame. {rows} rows loaded")

    return df


def filter_status_out(df_name, df, statuses):
    '''Filter out from DataFrame, orders that have status not required based on status id number.
                
        Args:
            df_name (str): Name of DataFrame
            df (DataFrame): DataFrame to filter
            statuses (list): List of status id's to be filtered out from DataFrame
        
        Returns:
            filtered_df (DataFrame)
        
        Raises:
            Exception:
        '''

    filtered_df = df.filter(~df.statusId.isin(statuses))
    rows = filtered_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Transactions with status {statuses} were filtered out. Row count is {rows}")
      
    return filtered_df


def categorize_transactions(df_name, df):
    '''Categorize transactions based on defined parameters.
                
        Args:
            df_name (str): Name of DataFrame
            df (DataFrame): DataFrame to categorize           
        
        Returns:
            categorized_df (DataFrame)
        
        Raises:
            Exception:
        '''

    categorized_df = df.select(df['*'], 
            F.when((df.num.like("%#SS%")) | (df.num.like("%#CS%")) | (df.num.like("%#MS%"))\
                | (df.num.like("%Samples%")) | (df.num.like("%SAMPLES%")) | (df.num.like("%samples%")), "Samples")
            .when(df.num.like("%RMA%"), "RMA")
            .when(df.customerId == c.id_15, "C")
            .when(df.customerId == c.id_14, "G")
            .when(df.customerId == c.id_13, "H")
            .when(df.customerId == c.id_12, "J")
            .when(df.customerId == c.id_11, "N")
            .when(df.customerId == c.id_10, "A")
            .when(df.customerId == c.id_9, "B")
            .when(df.customerId == c.id_8, "E")
            .when(df.customerId == c.id_7, "F")
            .when(df.customerId == c.id_6, "I")
            .when(df.customerId == c.id_5, "M")
            .when(df.customerId == c.id_4, "Q")
            .when(df.customerId == c.id_3, "R")
            .when((df.customerId == c.id_2) | (df.customerId == c.id_1), "Closed Channel")
            .when((df.qbClassId == 19) & (df.customerId != c.id_15) & (df.customerId != c.id_11), "K")
            .when((df.qbClassId == 9) & (df.customerId != c.id_7) & (df.customerId != c.id_5) & (df.customerId != c.id_12), "O")
            .when((df.qbClassId == 12) | (df.qbClassId == 17), "P")                                     
            .otherwise("Uncategorized").alias("sales_channel"))
    
    rows = categorized_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Sales transactions were categorized. Row count is {rows}")
 
    return categorized_df


def retrieve_not_req_trans(df, categories):
    '''Filter categories not required for Fact_Sales table and save them as CSV file.
                    
            Args:
                df (DataFrame): DataFrame to filter and save as CSV file
                categories (list): List of categories to be filtered           
            
            Returns:
                not_required_trans (DataFrame)
            
            Raises:
                Exception:
            '''
    
    not_required_trans = df.filter(df.sales_channel.isin(categories))
    rows = not_required_trans.count()
    logger.info(f"Transactions not required in Fact_Sales table were loaded into DataFrame 'not_required'. Row count is {rows}")
    
    return not_required_trans


def filter_categories_out(df_name, df, categories):
    '''Filter out categories not required for Fact_Sales table.
                    
            Args:
                df_name (str): Name of DataFrame
                df (DataFrame): DataFrame to filter
                categories (list): List of categories to be filtered out         
            
            Returns:
                filtered_df (DataFrame)
            
            Raises:
                Exception:
            '''
    
    filtered_df = df.select("id", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId", "sales_channel")\
                .filter(~df.sales_channel.isin(categories))
    rows = filtered_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Sales transactions with categories: {categories} were filtered out. Row count is {rows}")
       
    return filtered_df


def filter_item_type(df_name, df, item_type):
    '''Filter required item type.
                    
            Args:
                df_name (str): Name of DataFrame
                df (DataFrame): DataFrame to filter
                item_type (int): Item type to be filtered
            
            Returns:
                not_required (DataFrame)
            
            Raises:
                Exception:
            '''
    
    filtered_df = df.filter(df.typeId == item_type)
    rows = filtered_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Items with typeId {item_type} were filtered. Row count is {rows}")
       
    return filtered_df


def round_qty_ordered(df_name, df):
    '''Round quantity ordered to zero digits.
                    
            Args:
                df_name (str): Name of DataFrame
                df (DataFrame): DataFrame to round ordered quantity from         
            
            Returns:
                rounded_df (DataFrame)
            
            Raises:
                Exception:
            '''
    
    rounded_df = df.withColumn("qtyOrdered_r", F.round(df.qtyOrdered, 0)).drop(df.qtyOrdered)
    rows = rounded_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Column qtyOrdered was rounded to zero digits. Row count is {rows}")
       
    return rounded_df


def round_part_dims(df_name, df):
    '''Round part dimensions down to two digits.
                    
            Args:
                df_name (str): Name of DataFrame
                df (DataFrame): DataFrame to round part dimensions from          
            
            Returns:
                rounded_df (DataFrame)
            
            Raises:
                Exception:
            '''
    
    rounded_df = df.withColumn("len_r", F.round(df.len, 2)).drop(df.len)
    rounded_df = rounded_df.withColumn("width_r", F.round(rounded_df.width, 2)).drop(rounded_df.width)
    rounded_df = rounded_df.withColumn("height_r", F.round(rounded_df.height, 2)).drop(rounded_df.height)
    rows = rounded_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Columns: len, width and height were rounded to 2 digits. Row count is {rows}")
           
    return rounded_df


def create_fact_sales(soitem, so, product, part):
    '''Create Fact Sales table by joining multile DataFrames.
                    
            Args:
                soitem (DataFrame): soitem DataFrame
                so (DataFrame): so DataFrame
                product (DataFrame): product DataFrame
                part (DataFrame): part DataFrame
            
            Returns:
                Fact_Sales (DataFrame)
            
            Raises:
                Exception:
            '''
    
    # Join DataFrame 'soitem' with DataFrame 'so'
    categorized_items = soitem.join(so, soitem.soId ==  so.id)  
    rows = categorized_items.count()
    logger.info(f"Join completed. DataFrame 'soitem' joined with DataFrame 'so'. Row count is {rows}")

    # Join DataFrame 'categorized_items' with DataFrame 'product'
    categorized_items = categorized_items.join(product, categorized_items.productId == product.id)
    rows = categorized_items.count()
    logger.info(f"Join completed. DataFrame 'categorized_items' joined with DataFrame 'product'. Row count is {rows}")

    # Join DataFrame 'categorized_items' with DataFrame 'part'
    categorized_items = categorized_items.join(part, categorized_items.partId == part.id)
    rows = categorized_items.count()
    logger.info(f"Join completed. DataFrame 'categorized_items' joined with DataFrame 'part'. Row count is {rows}")

    # Fact Sales table
    Fact_Sales = categorized_items.select(categorized_items.dateCreated.alias("Date_Id"), categorized_items.sales_channel.alias("Sales_Channel_Id"), \
                        categorized_items.masked_num.alias("Product_Id"), categorized_items.qtyOrdered_r.alias("Units_Sold"))
    rows = Fact_Sales.count()                    
    logger.info(f"Fact Sales table was created. Row count is {rows}")
    
    return Fact_Sales


def create_dim_products(part):
    '''Create Dim Products table.
                    
            Args:               
                part (DataFrame): part DataFrame
            
            Returns:
                Dim_Products (DataFrame)
            
            Raises:
                Exception:
            '''
    

    Dim_Products = part.select("id", "masked_num", "len_r", "width_r", "height_r")
    rows = Dim_Products.count()                    
    logger.info(f"Dim Products table was created. Row count is {rows}")
    
    return Dim_Products


# Start SparkSession (entry point to Spark)
cleaning_session = SparkSession.builder.master("local[*]").appName('Data_Cleaning').getOrCreate()


# Read Parquet files into DataFrames
so = read_parquet("r_so")
soitem = read_parquet("r_soitem")
product = read_parquet("r_product")
part = read_parquet("m_part")

## 'so' DataFrame transformations

# Filter out orders that have order status not required (80: Voided, 85: Cancelled, 90: Expired, 95: Historical)
status_not_required = [80, 85, 90, 95]
so = filter_status_out("so", so, status_not_required)

# Categorize transactions by Sales Channel
so = categorize_transactions("so", so)

# Save transactions not required (Samples, RMA, Closed Channel, Uncategorized) in CSV file as backup
categories_not_req = ["Samples", "RMA", "Closed Channel", "Uncategorized"]
not_required_trans = retrieve_not_req_trans(so, categories_not_req)
not_required_trans.write.mode('overwrite').csv("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\transactions_not_required")
logger.info(f"DataFrame 'not_required' was saved as CSV file")


# Remove transactions not required from "so" DataFrame (Samples, RMA, Closed Channel, Uncategorized)
so = filter_categories_out("so", so, categories_not_req)


## 'soitem' DataFrame transformations

# Filter only items with typeId = 10 (Sale Items)
soitem = filter_item_type("soitem", soitem, 10)

# Round qtyOrdered field to zero digits
soitem = round_qty_ordered("soitem", soitem)

## 'part' DataFrame transformations

# Round fields: len, width and height to 2 digits
part = round_part_dims("part", part)


# Create and save 'Fact_Sales' table as Parquet file
Fact_Sales = create_fact_sales(soitem, so, product, part)
Fact_Sales.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Star_Schema_Tables\\Fact_Sales")
logger.info(f"Table 'Fact_Sales' was successfully saved as Parquet file")


# Create and save 'Dim_Products' table as Parquet file

Dim_Products = create_dim_products(part)
Dim_Products.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Star_Schema_Tables\\Dim_Products")
logger.info(f"Table Dim_Products was successfully saved as Parquet file")

# Create and save 'Dim_Sales_Channels' table as Parquet file
sales_cat_schema = StructType([
	StructField("id", StringType(), False),
    StructField("platform_type", StringType(), False),
    StructField("B2", StringType(), False),
    StructField("drop_shipping", BooleanType(), False),    
])

Dim_Sales_Channels = cleaning_session.read.option("header", True).schema(sales_cat_schema).csv("C:\\Users\\FBLServer\\Documents\\c\\sales_cat.csv")
Dim_Sales_Channels.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Star_Schema_Tables\\Dim_Sales_Channels")
logger.info(f"Table 'Dim_Sales_Channels' was successfully saved as Parquet file")

# Create and save 'Dim_Dates' table as Parquet file
date_schema = StructType([
	StructField("date", TimestampType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),    
])

Dim_Dates = cleaning_session.read.option("header", True).schema(date_schema).csv("C:\\Users\\FBLServer\\Documents\\c\\date.csv")
Dim_Dates.write.mode('overwrite').parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\DEV\\Output\\Star_Schema_Tables\\Dim_Dates")
logger.info(f"Table 'Dim_Dates' was successfully saved as Parquet file")

# Record script running time
# script_time = round(time.time() - star_time, 2)
# logger.info(f"Script running time was {script_time} secs")