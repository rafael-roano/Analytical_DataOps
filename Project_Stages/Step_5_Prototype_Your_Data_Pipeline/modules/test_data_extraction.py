import data_extraction as d
import pandas as pd
import logging
import pytest
import sys
sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
import mtest as m

LOGGER = logging.getLogger(__name__)

def test_table_to_dataframe_exception(spark_session, caplog):

    caplog.set_level(logging.INFO)

    table = "nonexistent_table"
    
    # Input dataframe
    # empty_df = spark_session.createDataFrame([], schema)
    # empty_df = spark_session.createDataFrame([], StructType([]))

    # SystemExit assertion
    with pytest.raises(SystemExit) as pytest_wrapped_e:
            d.table_to_dataframe(table)
    assert f"Unexpected error during '{table}' table reading from database. Unexpected error:" in caplog.text
    assert pytest_wrapped_e.type == SystemExit


def test_reduce_so_table(spark_session, caplog):

    caplog.set_level(logging.INFO)
       
    # Parameters
    so_req_fields = ["id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId"]
    field_count = 9
    start_date = "2021-01-01 00:00:00"
    finish_date = "2021-12-31 23:59:59"

    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(11215, "100 Sunset Ave", 2, "2970250", 7, 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 60, 2),
         (13783, "101 Moonrise St", 2, "#11558", 7, 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 80, 2),
         (13784, "102 Mars St", 2, "#11559b", 5, 1, 3852, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 85, 2),
         (13785, "103 Venus St", 2, "#11564", 5, 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 10, 2),
         (13786, "104 Earth St", 2, "#11563", 4, 1, 1731, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 95, 5),
         (13787, "105 Mercury St", 2, "#11562", 1, 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 5),
         (13788, "106 Jupyter St", 2, "#11561", 7, 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 90, 5)],
        ['id', 'billToAddress', 'billToCountryId', 'num', 'carrierId', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'paymentTermsId'],
    )

    expected_output = spark_session.createDataFrame(
        [(11215, "2970250", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 60),
         (13783, "#11558", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 80),
         (13784, "#11559b", 1, 3852, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 85),
         (13785, "#11564", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 10),
         (13786, "#11563", 1, 1731, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 95),
         (13787, "#11562", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20),
         (13788, "#11561", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 90)],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId'],
    )

    # Actual output dataframe
    actual_output = d.reduce_so_table(input, so_req_fields, field_count, start_date, finish_date)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Table 'so' was successfully reduced to required fields: 9. Row count is 7." in caplog.text


def test_reduce_so_table_fields_fail(spark_session, caplog):

    caplog.set_level(logging.INFO)
       
    # Parameters
    so_req_fields = ["id", "num", "currencyId", "customerId", "dateCompleted", "dateCreated", "locationGroupId", "qbClassId", "statusId"]
    field_count = 7
    start_date = "2021-01-01 00:00:00"
    finish_date = "2021-12-31 23:59:59"

    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(11215, "100 Sunset Ave", 2, "2970250", 7, 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 60, 2),
         (13783, "101 Moonrise St", 2, "#11558", 7, 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 80, 2),
         (13784, "102 Mars St", 2, "#11559b", 5, 1, 3852, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 85, 2),
         (13785, "103 Venus St", 2, "#11564", 5, 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 10, 2),
         (13786, "104 Earth St", 2, "#11563", 4, 1, 1731, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 95, 5),
         (13787, "105 Mercury St", 2, "#11562", 1, 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 5),
         (13788, "106 Jupyter St", 2, "#11561", 7, 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 90, 5)],
        ['id', 'billToAddress', 'billToCountryId', 'num', 'carrierId', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'paymentTermsId'],
    )
    
    # SystemExit assertion
    with pytest.raises(SystemExit) as pytest_wrapped_e:
            d.reduce_so_table(input, so_req_fields, field_count, start_date, finish_date)
    assert f"Table 'so' was not reduced to required fields. Program aborted." in caplog.text
    assert pytest_wrapped_e.type == SystemExit
    
    

def test_reduce_table(spark_session, caplog):

    caplog.set_level(logging.INFO)
       
    # Parameters
    table_name = "soitem"
    table_req_fields = ["id", "productId", "qtyOrdered", "soId", "typeId"]
    field_count = 5  

    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(1, '2021-01-05 00:00:00' , 518, 1, 1.000000000, 1, True, 10),
         (2, '2021-01-06 00:00:00', 520, 1, 1.000000000, 1, True, 60),
         (11, '2021-01-07 00:00:00', 637, 2, 3.000000000, 3, True, 11),
         (100, '2021-02-15 00:00:00', 1024, 2, 10.000000000, 100, True, 10),
         (200, '2021-03-20 00:00:00', 2754, 1, 1.000000000, 154, True, 31),                     
         (1000, '2021-01-04 00:00:00', 4521, 4, 1.000000000, 1325, True, 70)],
        ['id', 'dateLastFulfillment', 'productId', 'qbClassId', 'qtyOrdered', 'soId', 'showItemFlag', 'typeId'],
    )

    expected_output = spark_session.createDataFrame(
         [(1, 518, 1.000000000, 1, 10),
         (2, 520, 1.000000000, 1, 60),
         (11, 637, 3.000000000, 3, 11),
         (100, 1024, 10.000000000, 100, 10),
         (200, 2754, 1.000000000, 154, 31),                     
         (1000, 4521, 1.000000000, 1325, 70)],
        ['id', 'productId', 'qtyOrdered', 'soId', 'typeId'],
    )

    # Actual output dataframe
    actual_output = d.reduce_table(input, table_req_fields, field_count, table_name)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Table '{table_name}' was successfully reduced to required fields: 5. Row count is 6." in caplog.text


def test_reduce_table_fields_fail(spark_session, caplog):

    caplog.set_level(logging.INFO)
       
    # Parameters
    table_name = "soitem"
    table_req_fields = ["id", "productId", "qtyOrdered", "soId", "typeId"]
    field_count = 7 

    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(1, '2021-01-05 00:00:00' , 518, 1, 1.000000000, 1, True, 10),
         (2, '2021-01-06 00:00:00', 520, 1, 1.000000000, 1, True, 60),
         (11, '2021-01-07 00:00:00', 637, 2, 3.000000000, 3, True, 11),
         (100, '2021-02-15 00:00:00', 1024, 2, 10.000000000, 100, True, 10),
         (200, '2021-03-20 00:00:00', 2754, 1, 1.000000000, 154, True, 31),                     
         (1000, '2021-01-04 00:00:00', 4521, 4, 1.000000000, 1325, True, 70)],
        ['id', 'dateLastFulfillment', 'productId', 'qbClassId', 'qtyOrdered', 'soId', 'showItemFlag', 'typeId'],
    )
   
    # SystemExit assertion
    with pytest.raises(SystemExit) as pytest_wrapped_e:
            d.reduce_table(input, table_req_fields, field_count, table_name)
    assert f"Table '{table_name}' was not reduced to required fields. Program aborted." in caplog.text
    assert pytest_wrapped_e.type == SystemExit


def test_mask_part(spark_session, caplog):
       
    caplog.set_level(logging.INFO)
    
    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(2, 19.500000000, 10.000000000, m.u1, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}'),
         (3, 19.500000000, 10.000000000, m.u2, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}'),
         (4, 30.000000000, 8.000000000, m.u3, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}'),
         (5, 30.000000000, 8.000000000, m.u4, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}'),
         (6, 20.000000000, 15.000000000, m.u5, 10, 15.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}')],
        ['id', 'height', 'len', 'num', 'typeId', 'width', 'customFields'],
    )

    expected_output = spark_session.createDataFrame(
        [(2, 19.500000000, 10.000000000, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m1),
         (3, 19.500000000, 10.000000000, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m2),
         (4, 30.000000000, 8.000000000, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m3),
         (5, 30.000000000, 8.000000000, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m4),
         (6, 20.000000000, 15.000000000, 10, 15.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m5)],
        ['id', 'height', 'len', 'typeId', 'width', 'customFields', 'masked_num'],
    )
    
    # Actual output dataframe
    actual_output = input.withColumn("masked_num", d.mask_part(input.num)).drop(input.num)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )


def test_check_masking(spark_session, caplog):
       
    caplog.set_level(logging.INFO)

    field_count = 7
    
    # Input and expected output dataframes 
    input = spark_session.createDataFrame(
        [(2, 19.500000000, 10.000000000, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m1),
         (3, 19.500000000, 10.000000000, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m2),
         (4, 30.000000000, 8.000000000, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m3),
         (5, 30.000000000, 8.000000000, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m4),
         (6, 20.000000000, 15.000000000, 10, 15.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m5)],
        ['id', 'height', 'len', 'typeId', 'width', 'customFields', 'masked_num'],
    )
        
    d.check_masking(input, field_count)
    assert f"DataFrame 'reduced_part' was successfully masked." in caplog.text 


def test_check_masking_fail(spark_session, caplog):
       
    caplog.set_level(logging.INFO)

    field_count = 7
    
    # Input and expected output dataframes 
    input = spark_session.createDataFrame(
        [(2, 19.500000000, 10.000000000, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m1),
         (3, 19.500000000, 10.000000000, 10, 22.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m2),
         (4, 30.000000000, 8.000000000, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.u3),
         (5, 30.000000000, 8.000000000, 10, 24.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m4),
         (6, 20.000000000, 15.000000000, 10, 15.000000000, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', m.m5)],
        ['id', 'height', 'len', 'typeId', 'width', 'customFields', 'masked_num'],
    )
        
    # SystemExit assertion
    with pytest.raises(SystemExit) as pytest_wrapped_e:
            d.check_masking(input, field_count)
    assert f"DataFrame 'reduced_part' was not successfully masked. Program aborted." in caplog.text
    assert pytest_wrapped_e.type == SystemExit

