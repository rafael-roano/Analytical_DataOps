from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

# Paramaters

file_path = "/usr/local/spark/resources/data/airflow.cfg"

# Create Airflow DAG:

default_args = {
    "owner": "airflow",
    #"depends_on_past": False,
    #"email": ["airflow@airflow.com"],
    #"email_on_failure": False,
    #"email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3)
}

now = datetime.now()

with DAG("initial_etl_dag",
        start_date=datetime(now.year, now.month, now.day),
        schedule_interval = None,   # To trigger manually
        default_args=default_args,
        description = "Initial ETL of sales transactions", 
        catchup=False
        ) as dag:

    start = DummyOperator(task_id="start")

    # Spark Operator to submit initial_data_extraction.py
    mysql_data_extraction = SparkSubmitOperator(
                            task_id="mysql_data_extraction",
                            application="/usr/local/spark/app/initial_data_extraction.py",
                            conn_id="spark_default",
                            verbose=1,
                            jars="/usr/local/spark/resources/jars/mysql-connector-java-8.0.25.jar",
                            application_args=[file_path])
    

    # Spark Operator to submit initial_data_cleaning.py
    data_transformation = SparkSubmitOperator(
                            task_id="data_transformation",
                            application="/usr/local/spark/app/initial_data_cleaning.py",
                            conn_id="spark_default",
                            verbose=1,
                            application_args=[file_path])


    # SparkSubmitOperator to submit initial_data_upload.py
    S3_data_upload = SparkSubmitOperator(
                            task_id="S3_data_upload",
                            application="/usr/local/spark/app/initial_data_upload.py",
                            conn_id="spark_default",
                            verbose=1,
                            jars="/usr/local/spark/resources/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/resources/jars/aws-java-sdk-1.7.4.jar",
                            application_args=[file_path])

 
    # PythonOperator to get S3 file path to initial transfomed data
    def get_s3_paths(bucket_name, **kwargs):
                            hook = S3Hook("s3")
                            
                            fact_sales = hook.list_keys(bucket_name, prefix='Star_Schema_Tables/Fact_Sales/')
                            fact_sales_path = fact_sales[1]
                            kwargs['task_instance'].xcom_push(key='fact_sales_path', value=fact_sales_path)

                            dim_prod = hook.list_keys(bucket_name, prefix='Star_Schema_Tables/Dim_Products/')
                            dim_prod_path = dim_prod[1]
                            kwargs['task_instance'].xcom_push(key='dim_prod_path', value=dim_prod_path)

                            dim_chan = hook.list_keys(bucket_name, prefix='Star_Schema_Tables/Dim_Sales_Channels/')
                            dim_chan_path = dim_chan[1]
                            kwargs['task_instance'].xcom_push(key='dim_chan_path', value=dim_chan_path)

                            dim_dates = hook.list_keys(bucket_name, prefix='Star_Schema_Tables/Dim_Dates/')
                            dim_dates_path = dim_dates[1]
                            kwargs['task_instance'].xcom_push(key='dim_dates_path', value=dim_dates_path)

    get_S3_file_paths = PythonOperator(
                task_id = "get_S3_file_paths",
                provide_context = True,
                python_callable = get_s3_paths,
                op_kwargs={ "bucket_name" : "aaa-raw-data"})

        
    # SnowflakeOperator to pull transformed data (Parquet file) on S3 into Snowflake and copy data into FACT_SALES table
    fact_sales_snowflake_transfer = SnowflakeOperator(
                            task_id="fact_sales_snowflake_transfer",
                            snowflake_conn_id="snowflake",
                            sql="COPY INTO FACT_SALES FROM (SELECT $1:Date_Id::TIMESTAMP_NTZ(9), \
                                    $1:Sales_Channel_Id::VARCHAR(16777216), \
                                    $1:Product_Id::VARCHAR(16777216), \
                                    $1:Units_Sold::NUMBER(38,0) \
                                    FROM @S3_BUCKET/{{ task_instance.xcom_pull(key='fact_sales_path') }}) \
                                    on_error = 'skip_file' file_format = (type = parquet compression = SNAPPY)",
                            warehouse="COMPUTE_WH",
                            database="SALES",
                            schema="STAR_SCHEMA")

    dim_prod_snowflake_transfer = SnowflakeOperator(
                            task_id="dim_prod_snowflake_transfer",
                            snowflake_conn_id="snowflake",
                            sql="COPY INTO DIM_PRODUCTS FROM (SELECT $1:sku::VARCHAR(16777216), \
                                    $1:design_year::NUMBER(38,0), \
                                    $1:season::VARCHAR(16777216), \
                                    $1:material::VARCHAR(16777216), \
                                    $1:volume::FLOAT \
                                    FROM @S3_BUCKET/{{ task_instance.xcom_pull(key='dim_prod_path') }}) \
                                    on_error = 'skip_file' file_format = (type = parquet compression = SNAPPY)",
                            warehouse="COMPUTE_WH",
                            database="SALES",
                            schema="STAR_SCHEMA")

    dim_chan_snowflake_transfer = SnowflakeOperator(
                            task_id="dim_chan_snowflake_transfer",
                            snowflake_conn_id="snowflake",
                            sql="COPY INTO DIM_SALES_CHANNELS FROM (SELECT $1:sales_channel::VARCHAR(16777216), \
                                    $1:platform_type::VARCHAR(16777216), \
                                    $1:business_to::VARCHAR(16777216), \
                                    $1:drop_shipping::BOOLEAN \
                                    FROM @S3_BUCKET/{{ task_instance.xcom_pull(key='dim_chan_path') }}) \
                                    on_error = 'skip_file' file_format = (type = parquet compression = SNAPPY)",
                            warehouse="COMPUTE_WH",
                            database="SALES",
                            schema="STAR_SCHEMA")
    
    dim_dates_snowflake_transfer = SnowflakeOperator(
                            task_id="dim_dates_snowflake_transfer",
                            snowflake_conn_id="snowflake",
                            sql="COPY INTO DIM_DATE FROM (SELECT $1:date_ts::TIMESTAMP_NTZ(9), \
                                    $1:year::NUMBER(38,0), \
                                    $1:month::NUMBER(38,0), \
                                    $1:day::NUMBER(38,0) \
                                    FROM @S3_BUCKET/{{ task_instance.xcom_pull(key='dim_dates_path') }}) \
                                    on_error = 'skip_file' file_format = (type = parquet compression = SNAPPY)",
                            warehouse="COMPUTE_WH",
                            database="SALES",
                            schema="STAR_SCHEMA")
    
    
    # snowflake_transfer = S3ToSnowflakeOperator(
    #                         task_id="snowflake_transfer",
    #                         snowflake_conn_id="snowflake",
    #                         s3_keys=["Fact_Sales/part-00000-724b0a8e-72b2-4894-8357-d535278ccc59-c000.snappy.parquet"],
    #                         table="TEMP",
    #                         schema="STAR_SCHEMA",
    #                         stage="S3_BUCKET",
    #                         # file_format="(type = 'CSV',field_delimiter = ',',skip_header = 1)")
    #                         file_format="(type = 'PARQUET',compression = 'SNAPPY')")
    
    end = DummyOperator(task_id="end")

    # Job Dependencies

    start >> mysql_data_extraction >> data_transformation >> S3_data_upload >> get_S3_file_paths >> fact_sales_snowflake_transfer >> end
    start >> mysql_data_extraction >> data_transformation >> S3_data_upload >> get_S3_file_paths >> dim_prod_snowflake_transfer >> end
    start >> mysql_data_extraction >> data_transformation >> S3_data_upload >> get_S3_file_paths >> dim_chan_snowflake_transfer >> end
    start >> mysql_data_extraction >> data_transformation >> S3_data_upload >> get_S3_file_paths >> dim_dates_snowflake_transfer >> end
    