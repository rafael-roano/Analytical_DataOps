from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import time


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
logger = logging.getLogger("daily_data_cleaning_aws")
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


def read_daily_parquet(file):

    df = cleaning_session.read.parquet(f"s3a://aaa-raw-data/Extracted_MySQL_Tables/daily_transactions/{file}")
    rows = df.count()
    logger.info(f"Parquet file '{file}' was successfully loaded into DataFrame. {rows} rows loaded")

    return df
    

def read_parquet(file):

    df = cleaning_session.read.parquet(f"s3a://aaa-raw-data/Extracted_MySQL_Tables/initial_extraction/{file}")
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
            .when(df.customerId == 3738, "C")
            .when(df.customerId == 1731, "G")
            .when(df.customerId == 1, "H")
            .when(df.customerId == 6342, "J")
            .when(df.customerId == 3806, "N")
            .when(df.customerId == 4854, "A")
            .when(df.customerId == 3995, "B")
            .when(df.customerId == 426, "E")
            .when(df.customerId == 312, "F")
            .when(df.customerId == 2839, "I")
            .when(df.customerId == 3809, "M")
            .when(df.customerId == 6343, "Q")
            .when(df.customerId == 3188, "R")
            .when((df.customerId == 3852) | (df.customerId == 1159), "Closed Channel")
            .when((df.qbClassId == 19) & (df.customerId != 3738) & (df.customerId != 3806), "K")
            .when((df.qbClassId == 9) & (df.customerId != 312) & (df.customerId != 3809) & (df.customerId != 6342), "O")
            .when((df.qbClassId == 12) | (df.qbClassId == 17), "P")                                     
            .otherwise("Uncategorized").alias("sales_channel"))
    
    rows = categorized_df.count()
    logger.info(f"Transformation on DataFrame '{df_name}' completed: Sales transactions were categorized. Row count is {rows}")
 
    return categorized_df


def retrieve_not_req_trans(df, categories):
    '''Filter categories not required for Fact_Sales table.
                    
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


# Start SparkSession (entry point to Spark)
cleaning_session = SparkSession.builder.appName('daily_data_cleaning_aws').getOrCreate()


# Read Parquet files into DataFrames
so = read_daily_parquet("r_so")
soitem = read_daily_parquet("r_soitem")
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
not_required_trans.write.mode('overwrite').csv("s3a://aaa-raw-data/Extracted_MySQL_Tables/daily_transactions/transactions_not_loaded.csv")
logger.info(f"DataFrame 'not_required' was saved as CSV file")

# Remove transactions not required from "so" DataFrame (Samples, RMA, Closed Channel, Uncategorized)
so = filter_categories_out("so", so, categories_not_req)



## 'soitem' DataFrame transformations

# Filter only items with typeId = 10 (Sale Items)
soitem = filter_item_type("soitem", soitem, 10)

# Round qtyOrdered field to zero digits
soitem = round_qty_ordered("soitem", soitem)



# Create and save 'Fact_Sales' table as Parquet file
Fact_Sales = create_fact_sales(soitem, so, product, part)
Fact_Sales.write.mode('overwrite').parquet("s3a://aaa-raw-data/Star_Schema_Tables/Fact_Sales")
logger.info(f"Table 'Fact_Sales' was successfully saved as Parquet file")


# Record script running time
script_time = round(time.time() - star_time, 2)
logger.info(f"'daily_data_cleaning_aws' script was successfully executed. Runnig time was {script_time} secs")