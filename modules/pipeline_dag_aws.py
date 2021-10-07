from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

from datetime import datetime, timedelta


# Paramaters

file_path = "/usr/local/spark/resources/data/airflow.cfg"

emr_cluster_id = "j-"

SPARK_STEPS = [
#     {
#         "Name": "Move initial_data_cleaning_aws.py from S3 to HDFS",
#         "ActionOnFailure": "CANCEL_AND_WAIT",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": [
#                 "s3-dist-cp",
#                 "--src=s3a://aaa-raw-data/resources/x/initial_data_cleaning_aws.py", 
#                 "--dest=/resources",
#             ],
#         },
#     },

    # {
    #     "Name": "Move pipeline.log from S3 to HDFS",
    #     "ActionOnFailure": "CANCEL_AND_WAIT",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "s3-dist-cp",
    #             "--src=s3://aaa-raw-data/logs/pipeline.log",
    #             "--dest=s3://aaa-raw-data/",
    #         ],
    #     },
    # }, 

    {
        "Name": "Run daily_data_cleaning_aws.py spark job",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3a://aaa-raw-data/resources/x/daily_data_cleaning_aws.py",
            ],
        },
    },
   
]

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

with DAG("pipeline_dag_aws",
        start_date=datetime(2021, 10, 6),
        schedule_interval = "0 8 * * *",   # Run daily at 08:00 AM (UTC); equivlent to 01:00 AM (PDT)
        default_args=default_args,
        description = "Daily ETL of sales transactions (aws)", 
        catchup=True
        ) as dag:

    start = DummyOperator(task_id="start")

    # Spark Operator to submit daily_data_extraction_aws.py
    mysql_data_extraction = SparkSubmitOperator(
                            task_id="initial_data_extraction",
                            application="/usr/local/spark/app/daily_data_extraction_aws.py",
                            conn_id="spark_default",
                            verbose=1,
                            jars="/usr/local/spark/resources/jars/mysql-connector-java-8.0.25.jar,/usr/local/spark/resources/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/resources/jars/aws-java-sdk-1.7.4.jar",
                            application_args=[file_path])
                            
    
    # EmrAddSteps Operator to add steps to EMR cluster
    cluster_steps_addition = EmrAddStepsOperator(
        task_id="cluster_steps_addition",
        job_flow_id=emr_cluster_id,
        aws_conn_id="s3",
        steps=SPARK_STEPS)
    
    
    # Wait for the steps to complete
    last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
    step_check = EmrStepSensor(
        task_id="step_check",
        job_flow_id=emr_cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='cluster_steps_addition', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="s3")


    #     # Spark Operator to submit initial_data_cleaning.py
    #     data_transformation = SparkSubmitOperator(
    #                             task_id="data_transformation",
    #                             application="/usr/local/spark/app/initial_data_cleaning.py",
    #                             conn_id="spark_default",
    #                             verbose=1,
    #                             application_args=[file_path])


    #     # SparkSubmitOperator to submit initial_data_upload.py
    #     S3_data_upload = SparkSubmitOperator(
    #                             task_id="S3_data_upload",
    #                             application="/usr/local/spark/app/initial_data_upload.py",
    #                             conn_id="spark_default",
    #                             verbose=1,
    #                             jars="/usr/local/spark/resources/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/resources/jars/aws-java-sdk-1.7.4.jar",
    #                             application_args=[file_path])

 
    # PythonOperator to get S3 file path to initial transfomed data
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

        
    # SnowflakeOperator to pull transformed data (Parquet file) on S3 into Snowflake and copy data into FACT_SALES table
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

    start >> mysql_data_extraction >> cluster_steps_addition >> step_check >> get_S3_file_path >> snowflake_transfer >> end


  
    