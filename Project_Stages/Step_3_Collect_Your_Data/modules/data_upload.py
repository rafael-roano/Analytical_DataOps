from sqlalchemy import create_engine
import boto3
import pandas as pd
import time

'''
Module to extract data from MySQL database (transactional data), save it into csv files and upload them to an s3 bucket.
Scheduled to run once a day in tower server using Windows task scheduler.
'''

# Start timer for script running time
star_time = time.time()

# Interface to MySQL database
engine = create_engine("mysql+pymysql://" + "" + "@localhost:3305/" + "")

# Read tables from database into dataframes
so = pd.read_sql_table("so", engine)
soitem = pd.read_sql_table("soitem", engine)
customer = pd.read_sql_table("customer", engine)
qbclass = pd.read_sql_table("qbclass", engine)
part = pd.read_sql_table("part", engine)
product = pd.read_sql_table("product", engine)

# Save dataframes as csv files on tower server
so.to_csv("C:\\Users\\FBL Server\\Documents\\Python\\SB\\so.csv", index = False)
print("so.csv successfully saved on server")
soitem.to_csv("C:\\Users\\FBL Server\\Documents\\Python\\SB\\soitem.csv", index = False)
print("soitem successfully saved on server")
customer.to_csv("C:\\Users\\FBL Server\\Documents\\Python\\SB\\customer.csv", index = False)
print("customer.csv successfully saved on server")
qbclass.to_csv("C:\\Users\\FBL Server\\Documents\\Python\\SB\\qbclass.csv", index = False)
print("qbclass.csv successfully saved on server")
part.to_csv("C:\\Users\\FBL Server\\Documents\\Python\\SB\\part.csv", index = False)
print("part.csv successfully saved on server")
product.to_csv("C:\\Users\\FBL Server\\Documents\\Python\\SB\\product.csv", index = False)
print("product.csv successfully saved on server")
print()


# Upload csv files to s3 bucket
s3 = boto3.resource("s3")

s3.meta.client.upload_file("C:\\Users\\FBL Server\\Documents\\Python\\SB\\so.csv", "aaa-raw-data", "so.csv")
print("so.csv successfully uploaded to aaa-raw-data bucket")
s3.meta.client.upload_file("C:\\Users\\FBL Server\\Documents\\Python\\SB\\soitem.csv", "aaa-raw-data", "soitem.csv")
print("soitem.csv successfully uploaded to aaa-raw-data bucket")
s3.meta.client.upload_file("C:\\Users\\FBL Server\\Documents\\Python\\SB\\customer.csv", "aaa-raw-data", "customer.csv")
print("customer.csv successfully uploaded to aaa-raw-data bucket")
s3.meta.client.upload_file("C:\\Users\\FBL Server\\Documents\\Python\\SB\\qbclass.csv", "aaa-raw-data", "qbclass.csv")
print("qbclass.csv successfully uploaded to aaa-raw-data bucket")
s3.meta.client.upload_file("C:\\Users\\FBL Server\\Documents\\Python\\SB\\part.csv", "aaa-raw-data", "part.csv")
print("part.csv successfully uploaded to aaa-raw-data bucket")
s3.meta.client.upload_file("C:\\Users\\FBL Server\\Documents\\Python\\SB\\product.csv", "aaa-raw-data", "product.csv")
print("product.csv successfully uploaded to aaa-raw-data bucket")


# Calculate and print script running time
script_time = round(time.time() - star_time, 2)
print()
print(f"Script took {script_time} seconds")