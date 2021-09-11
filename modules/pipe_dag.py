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
    "depends_on_past": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

now = datetime.now()

with DAG("pipe_test",
        start_date=datetime(now.year, now.month, now.day),
        schedule_interval=timedelta(1),
        default_args=default_args, 
        catchup=False
        ) as dag:

    start = DummyOperator(task_id="start")

    # Spark Operator to submit data_extraction_spark.py
    mysql_data_extraction = SparkSubmitOperator(
                            task_id="mysql_data_extraction",
                            application="/usr/local/spark/app/data_extraction_spark.py",
                            conn_id="spark_default",
                            verbose=1,
                            jars="/usr/local/spark/resources/jars/mysql-connector-java-8.0.25.jar",
                            application_args=[file_path])
    

    # Spark Operator to submit data_cleaning_spark.py
    data_transformation = SparkSubmitOperator(
                            task_id="data_transformation",
                            application="/usr/local/spark/app/data_cleaning_spark.py",
                            conn_id="spark_default",
                            verbose=1,
                            application_args=[file_path])


    # SparkSubmitOperator to submit data_uploading_spark.py
    
    upload_tables_S3 = SparkSubmitOperator(
                            task_id="upload_tables_S3",
                            application="/usr/local/spark/app/data_uploading_spark.py",
                            # name="Data_Cleaning",
                            conn_id="spark_default",
                            verbose=1,
                            jars="/usr/local/spark/resources/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/resources/jars/aws-java-sdk-1.7.4.jar",
                            application_args=[file_path])

 
    # PythonOpe

    def get_s3_path(bucket_name, **kwargs):
                            hook = S3Hook("s3")
                            keys = hook.list_keys(bucket_name, prefix='Star_Schema_Tables/Fact_Sales/')
                            file_path = keys[1]
                            print(file_path)
                            kwargs['task_instance'].xcom_push(key='file_path', value=file_path)

    get_S3_file_path = PythonOperator(
                task_id = "get_S3_file_path",
                provide_context = True,
                python_callable = get_s3_path,
                op_kwargs={ "bucket_name" : "aaa-raw-data"})


    # snowflake_transfer = S3ToSnowflakeOperator(
    #                         task_id="snowflake_transfer",
    #                         snowflake_conn_id="snowflake",
    #                         s3_keys=["Fact_Sales/part-00000-724b0a8e-72b2-4894-8357-d535278ccc59-c000.snappy.parquet"],
    #                         table="TEMP",
    #                         schema="STAR_SCHEMA",
    #                         stage="S3_BUCKET",
    #                         # file_format="(type = 'CSV',field_delimiter = ',',skip_header = 1)")
    #                         file_format="(type = 'PARQUET',compression = 'SNAPPY')")
    
    snowflake_transfer = SnowflakeOperator(
                            task_id="snowflake_transfer",
                            snowflake_conn_id="snowflake",
                            sql="COPY INTO FACT_SALES FROM (SELECT $1:Date_Id::TIMESTAMP_NTZ(9), \
                                                    $1:Sales_Channel_Id::VARCHAR(16777216), \
                                                    $1:Product_Id::VARCHAR(16777216), \
                                                    $1:Units_Sold::NUMBER(38,0) \
                                                    FROM @S3_BUCKET/{{ task_instance.xcom_pull(key='file_path') }}) \
                                                    on_error = 'skip_file' file_format = (type = parquet compression = SNAPPY)",
                            warehouse="COMPUTE_WH",
                            database="SALES",
                            schema="STAR_SCHEMA")
    
    
    
    end = DummyOperator(task_id="end")

    # Job Dependencies
    start >> mysql_data_extraction >> data_transformation >> upload_tables_S3 >> get_S3_file_path >> snowflake_transfer >> end