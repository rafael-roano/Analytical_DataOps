import logging
import boto3
import os

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

# Upload Parquet files to s3 bucket
s3 = boto3.resource("s3")

# Delete existing star_schema directory
bucket = s3.Bucket("aaa-raw-data")
bucket.objects.filter(Prefix="star_schema/").delete()

# Upload Parquet Files
def uploadParquet(path, parquet):
    
    for root, dirs, files in os.walk(path):
        for file in files:
            s3.meta.client.upload_file(os.path.join(root, file), "aaa-raw-data", "star_schema/" + parquet + "/" + file)
    
    logger.info(f"Parquet file '{parquet}' was successfully loaded to S3")


uploadParquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Fact_Sales", "Fact_Sales")
uploadParquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Dim_Sales_Channels", "Dim_Sales_Channels")
uploadParquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Dim_Products", "Dim_Products")
uploadParquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Output\\Star_Schema_Tables\\Dim_Dates", "Dim_Dates")

# Record script running time
# script_time = round(time.time() - star_time, 2)
# logger.info(f"Script runnig time was {script_time} secs")