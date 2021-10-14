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

emr_cluster_id = "j-2TMS444F8O9UA"

SPARK_STEPS = [
    {
        "Name": "Run initial_data_cleaning_aws.py spark job",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3a://aaa-raw-data/resources/x/initial_data_cleaning_aws.py",
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

with DAG("initial_etl_dag_aws",
        start_date=datetime(now.year, now.month, now.day),
        schedule_interval = None,   # To trigger manually
        default_args=default_args,
        description = "Initial ETL of sales transactions (aws)", 
        catchup=False
        ) as dag:

    start = DummyOperator(task_id="start")

    # Spark Operator to submit initial_data_extraction.py
    mysql_data_extraction = SparkSubmitOperator(
                            task_id="initial_data_extraction",
                            application="/usr/local/spark/app/initial_data_extraction_aws.py",
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
    
    
    # Wait for the step to be completed
    last_step = len(SPARK_STEPS) - 1
    step_check = EmrStepSensor(
        task_id="step_check",
        job_flow_id=emr_cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='cluster_steps_addition', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="s3")

     
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
                                    $1:design_year::VARCHAR(16777216), \
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
    
          
    end = DummyOperator(task_id="end")

    # Job Dependencies

    start >> mysql_data_extraction >> cluster_steps_addition >> step_check >> get_S3_file_paths >> fact_sales_snowflake_transfer >> end
    start >> mysql_data_extraction >> cluster_steps_addition >> step_check >> get_S3_file_paths >> dim_prod_snowflake_transfer >> end
    start >> mysql_data_extraction >> cluster_steps_addition >> step_check >> get_S3_file_paths >> dim_chan_snowflake_transfer >> end
    start >> mysql_data_extraction >> cluster_steps_addition >> step_check >> get_S3_file_paths >> dim_dates_snowflake_transfer >> end

  
    