import time
import boto3
star_time = time.time()

'''
Module to download CSV files (transactional data) from an s3 bucket and save them in local machine.
'''

# Start timer for script running time
star_time = time.time()

# Download csv files to local machine from s3 bucket
s3 = boto3.resource("s3")
s3.meta.client.download_file("aaa-raw-data", "so.csv", "/home/jv/Python/SB/SB_Projects/Open-ended_Capstone/data/so.csv")
print("so.csv successfully downloaded to local machine from aaa-raw-data bucket")
s3.meta.client.download_file("aaa-raw-data", "soitem.csv", "/home/jv/Python/SB/SB_Projects/Open-ended_Capstone/data/soitem.csv")
print("soitem.csv successfully downloaded to local machine from aaa-raw-data bucket")
s3.meta.client.download_file("aaa-raw-data", "customer.csv", "/home/jv/Python/SB/SB_Projects/Open-ended_Capstone/data/customer.csv")
print("customer.csv successfully downloaded to local machine from aaa-raw-data bucket")
s3.meta.client.download_file("aaa-raw-data", "qbclass.csv", "/home/jv/Python/SB/SB_Projects/Open-ended_Capstone/data/qbclass.csv")
print("qbclass.csv successfully downloaded to local machine from aaa-raw-data bucket")
s3.meta.client.download_file("aaa-raw-data", "part.csv", "/home/jv/Python/SB/SB_Projects/Open-ended_Capstone/data/part.csv")
print("part.csv successfully downloaded to local machine from aaa-raw-data bucket")
s3.meta.client.download_file("aaa-raw-data", "product.csv", "/home/jv/Python/SB/SB_Projects/Open-ended_Capstone/data/product.csv")
print("product.csv successfully downloaded to local machine from aaa-raw-data bucket")

# Calculate and print script running time
script_time = round(time.time() - star_time, 2)
print()
print(f"Script took {script_time} seconds")