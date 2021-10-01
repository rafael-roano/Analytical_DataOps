import daily_data_cleaning as d
import pyspark.sql.functions as F
import pandas as pd
import sys
sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
import config as c
import logging


LOGGER = logging.getLogger(__name__)

def get_sorted_data_frame(data_frame, columns_list):

    return data_frame.sort_values(columns_list).reset_index(drop=True)


def test_filter_status_out(spark_session, caplog):

    caplog.set_level(logging.INFO)
    
    df_name = "so"

    # Status Id's to be filtered out
    status_not_required = [80, 85, 90, 95]

    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(11215, "2970250", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 60),
         (13783, "#11558", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 80),
         (13784, "#11559b", 1, 3852, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 85),
         (13785, "#11564", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 10),
         (13786, "#11563", 1, 1731, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 95),
         (13787, "#11562", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20),
         (13788, "#11561", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 90)],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId'],
    )

    expected_output = spark_session.createDataFrame(
        [(11215, "2970250", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 60),
         (13785, "#11564", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 10),
         (13787, "#11562", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20)],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId'],
    )

    # Actual output dataframe
    actual_output = d.filter_status_out(df_name, input, status_not_required)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Transformation on DataFrame '{df_name}' completed: Transactions with status {status_not_required} were filtered out. Row count is 3" in caplog.text


def test_categorize_transactions(spark_session, caplog):

    caplog.set_level(logging.INFO)
    
    df_name = "so"

    # Categorization paramenters
    id_15 = c.id_15
    id_14 = c.id_14
    id_13 = c.id_13
    id_12 = c.id_12
    id_11 = c.id_11
    id_10 = c.id_10
    id_9 = c.id_9
    id_8 = c.id_8
    id_7 = c.id_7
    id_6 = c.id_6
    id_5 = c.id_5
    id_4 = c.id_4
    id_3 = c.id_3
    id_2 = c.id_2
    id_1 = c.id_1

    # Input and expected output dataframes    
    input = spark_session.createDataFrame(
        [(11215, "#SS007", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 20),
         (13783, "RMA11100", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20),
         (13784, "#11559b", 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20),
         (13785, "#MS120", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20),
         (13786, "#11563", 1, id_14, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 20),
         (13787, "#11562", 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20),         
         (14600, "#20700", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14601, "#20701", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14602, "#20702", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14603, "#20703", 1, id_9, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14604, "#20704", 1, id_8, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14605, "#20705", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14606, "#20706", 1, id_6, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14607, "#20707", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14608, "#20708", 1, id_4, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14609, "#20709", 1, id_3, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14610, "#20710", 1, id_2, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14611, "#20711", 1, id_1, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14612, "#SamplesX", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14613, "001RMA", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20),
         (14614, "#20714", 1, 3701, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20),
         (14615, "#20715", 1, id_15, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20),
         (14616, "#20716", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20),
         (14617, "#20717", 1, 130, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20),
         (14618, "#20718", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20),
         (14619, "#20719", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20),
         (14620, "#20720", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20),
         (14621, "#20721", 1, 920, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 17, 20),
         (14622, "#20722", 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20),
         (14025, "#11841", 1, 734, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 7, 60),
         (15924, "#13508", 1, 6401, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 10, 60),
         (17029, "SHOWROOM", 1, 5426, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60),
         (14614, "PO1988", 1, 5923, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60),         
         (13788, "#CS100", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20)],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId'],
    )

    expected_output = spark_session.createDataFrame(
        [(11215, "#SS007", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 20, 'Samples'),
         (13783, "RMA11100", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20,'RMA'),
         (13784, "#11559b", 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C'),
         (13785, "#MS120", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples'),
         (13786, "#11563", 1, id_14, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'G'),
         (13787, "#11562", 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H'),
         (14600, "#20700", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'J'),
         (14601, "#20701", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N'),
         (14602, "#20702", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'A'),
         (14603, "#20703", 1, id_9, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'B'),
         (14604, "#20704", 1, id_8, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'E'),
         (14605, "#20705", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'F'),
         (14606, "#20706", 1, id_6, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'I'),
         (14607, "#20707", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'M'),
         (14608, "#20708", 1, id_4, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Q'),
         (14609, "#20709", 1, id_3, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'R'),
         (14610, "#20710", 1, id_2, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20,  'Closed Channel'),
         (14611, "#20711", 1, id_1, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Closed Channel'),
         (14612, "#SamplesX", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Samples'),
         (14613, "001RMA", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'RMA'),
         (14614, "#20714", 1, 3701, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'K'),
         (14615, "#20715", 1, id_15, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'C'),
         (14616, "#20716", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'N'),
         (14617, "#20717", 1, 130, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'O'),
         (14618, "#20718", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'F'),
         (14619, "#20719", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'M'),
         (14620, "#20720", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'J'),
         (14621, "#20721", 1, 920, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 17, 20, 'P'),
         (14622, "#20722", 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20, 'P'),
         (14025, "#11841", 1, 734, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 7, 60, 'Uncategorized'),
         (15924, "#13508", 1, 6401, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 10, 60, 'Uncategorized'),
         (17029, "SHOWROOM", 1, 5426, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),
         (14614, "PO1988", 1, 5923, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),         
         (13788, "#CS100", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples')],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel'],
    )
    
    # Actual output dataframe
    actual_output = d.categorize_transactions(df_name, input)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )
    
    assert f"Transformation on DataFrame '{df_name}' completed: Sales transactions were categorized. Row count is 34" in caplog.text


def test_retrieve_not_req_trans(spark_session, caplog):

    caplog.set_level(logging.INFO)
    
    # Categories to filter and save as transactions not required
    categories_not_req = ["Samples", "RMA", "Closed Channel", "Uncategorized"]

    # Categorization paramenters
    id_15 = c.id_15
    id_14 = c.id_14
    id_13 = c.id_13
    id_12 = c.id_12
    id_11 = c.id_11
    id_10 = c.id_10
    id_9 = c.id_9
    id_8 = c.id_8
    id_7 = c.id_7
    id_6 = c.id_6
    id_5 = c.id_5
    id_4 = c.id_4
    id_3 = c.id_3
    id_2 = c.id_2
    id_1 = c.id_1

    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(11215, "#SS007", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 20, 'Samples'),
         (13783, "RMA11100", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20,'RMA'),
         (13784, "#11559b", 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C'),
         (13785, "#MS120", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples'),
         (13786, "#11563", 1, id_14, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'G'),
         (13787, "#11562", 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H'),
         (14600, "#20700", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'J'),
         (14601, "#20701", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N'),
         (14602, "#20702", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'A'),
         (14603, "#20703", 1, id_9, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'B'),
         (14604, "#20704", 1, id_8, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'E'),
         (14605, "#20705", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'F'),
         (14606, "#20706", 1, id_6, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'I'),
         (14607, "#20707", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'M'),
         (14608, "#20708", 1, id_4, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Q'),
         (14609, "#20709", 1, id_3, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'R'),
         (14610, "#20710", 1, id_2, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20,  'Closed Channel'),
         (14611, "#20711", 1, id_1, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Closed Channel'),
         (14612, "#SamplesX", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Samples'),
         (14613, "001RMA", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'RMA'),
         (14614, "#20714", 1, 3701, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'K'),
         (14615, "#20715", 1, id_15, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'C'),
         (14616, "#20716", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'N'),
         (14617, "#20717", 1, 130, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'O'),
         (14618, "#20718", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'F'),
         (14619, "#20719", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'M'),
         (14620, "#20720", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'J'),
         (14621, "#20721", 1, 920, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 17, 20, 'P'),
         (14622, "#20722", 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20, 'P'),
         (14025, "#11841", 1, 734, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 7, 60, 'Uncategorized'),
         (15924, "#13508", 1, 6401, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 10, 60, 'Uncategorized'),
         (17029, "SHOWROOM", 1, 5426, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),
         (14614, "PO1988", 1, 5923, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),         
         (13788, "#CS100", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples')],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel'],
    )

    expected_output = spark_session.createDataFrame(
        [(11215, "#SS007", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 20, 'Samples'),
         (13783, "RMA11100", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20,'RMA'),
         (13785, "#MS120", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples'),
         (14610, "#20710", 1, id_2, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20,  'Closed Channel'),
         (14611, "#20711", 1, id_1, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Closed Channel'),
         (14612, "#SamplesX", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Samples'),
         (14613, "001RMA", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'RMA'),
         (14025, "#11841", 1, 734, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 7, 60, 'Uncategorized'),
         (15924, "#13508", 1, 6401, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 10, 60, 'Uncategorized'),
         (17029, "SHOWROOM", 1, 5426, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),
         (14614, "PO1988", 1, 5923, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),         
         (13788, "#CS100", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples')],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel'],
    )

    # Actual output dataframe
    actual_output = d.retrieve_not_req_trans(input, categories_not_req)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )
    
    assert f"Transactions not required in Fact_Sales table were loaded into DataFrame 'not_required'. Row count is 12" in caplog.text


def test_filter_categories_out(spark_session, caplog):
    
    caplog.set_level(logging.INFO)
    
    df_name = "so"

    # Categories to filter out
    categories_not_req = ["Samples", "RMA", "Closed Channel", "Uncategorized"]

    # Categorization paramenters
    id_15 = c.id_15
    id_14 = c.id_14
    id_13 = c.id_13
    id_12 = c.id_12
    id_11 = c.id_11
    id_10 = c.id_10
    id_9 = c.id_9
    id_8 = c.id_8
    id_7 = c.id_7
    id_6 = c.id_6
    id_5 = c.id_5
    id_4 = c.id_4
    id_3 = c.id_3
    id_2 = c.id_2
    id_1 = c.id_1

     # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(11215, "#SS007", 1, 559, "2021-02-02 00:00:00", "2021-02-05 00:00:00", 1, 12, 20, 'Samples'),
         (13783, "RMA11100", 1, 1, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20,'RMA'),
         (13784, "#11559b", 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C'),
         (13785, "#MS120", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples'),
         (13786, "#11563", 1, id_14, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'G'),
         (13787, "#11562", 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H'),
         (14600, "#20700", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'J'),
         (14601, "#20701", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N'),
         (14602, "#20702", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'A'),
         (14603, "#20703", 1, id_9, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'B'),
         (14604, "#20704", 1, id_8, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'E'),
         (14605, "#20705", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'F'),
         (14606, "#20706", 1, id_6, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'I'),
         (14607, "#20707", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'M'),
         (14608, "#20708", 1, id_4, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Q'),
         (14609, "#20709", 1, id_3, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'R'),
         (14610, "#20710", 1, id_2, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20,  'Closed Channel'),
         (14611, "#20711", 1, id_1, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Closed Channel'),
         (14612, "#SamplesX", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Samples'),
         (14613, "001RMA", 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'RMA'),
         (14614, "#20714", 1, 3701, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'K'),
         (14615, "#20715", 1, id_15, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'C'),
         (14616, "#20716", 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'N'),
         (14617, "#20717", 1, 130, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'O'),
         (14618, "#20718", 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'F'),
         (14619, "#20719", 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'M'),
         (14620, "#20720", 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'J'),
         (14621, "#20721", 1, 920, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 17, 20, 'P'),
         (14622, "#20722", 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20, 'P'),
         (14025, "#11841", 1, 734, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 7, 60, 'Uncategorized'),
         (15924, "#13508", 1, 6401, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 10, 60, 'Uncategorized'),
         (17029, "SHOWROOM", 1, 5426, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),
         (14614, "PO1988", 1, 5923, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 1, 60, 'Uncategorized'),         
         (13788, "#CS100", 1, 3701, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 19, 20, 'Samples')],
        ['id', 'num', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel'],
    )

    expected_output = spark_session.createDataFrame(
        [(13784, 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C'),         
         (13786, 1, id_14, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'G'),
         (13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H'),
         (14600, 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'J'),
         (14601, 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N'),
         (14602, 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'A'),
         (14603, 1, id_9, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'B'),
         (14604, 1, id_8, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'E'),
         (14605, 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'F'),
         (14606, 1, id_6, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'I'),
         (14607, 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'M'),
         (14608, 1, id_4, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Q'),
         (14609, 1, id_3, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'R'),         
         (14614, 1, 3701, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'K'),
         (14615, 1, id_15, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'C'),
         (14616, 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'N'),
         (14617, 1, 130, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'O'),
         (14618, 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'F'),
         (14619, 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'M'),
         (14620, 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'J'),
         (14621, 1, 920, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 17, 20, 'P'),
         (14622, 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20, 'P')],
        ['id', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel'],
    )

    # Actual output dataframe
    actual_output = d.filter_categories_out(df_name, input, categories_not_req)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Transformation on DataFrame '{df_name}' completed: Sales transactions with categories: {categories_not_req} were filtered out. Row count is 22" in caplog.text


def test_filter_item_type(spark_session, caplog):
    
    caplog.set_level(logging.INFO)

    df_name = "soitem"
    item_type = 10
  
    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(1, 518, 1.000000000, 1, 10),
         (2, 520, 1.000000000, 1, 60),
         (11, 637, 3.000000000, 3, 11),
         (100, 1024, 10.000000000, 100, 10),
         (200, 2754, 1.000000000, 154, 31),                     
         (1000, 4521, 1.000000000, 1325, 70)],
        ['id', 'productId', 'qtyOrdered', 'soId', 'typeId'],
    )

    expected_output = spark_session.createDataFrame(
        [(1, 518, 1.000000000, 1, 10),
         (100, 1024, 10.000000000, 100, 10)],
        ['id', 'productId', 'qtyOrdered', 'soId', 'typeId'],
    )

    # Actual output dataframe
    actual_output = d.filter_item_type(df_name, input, item_type)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Transformation on DataFrame '{df_name}' completed: Items with typeId {item_type} were filtered. Row count is 2" in caplog.text


def test_round_qty_ordered(spark_session, caplog):
    
    caplog.set_level(logging.INFO)

    df_name = "soitem"
  
    # Input and expected output dataframes
    input = spark_session.createDataFrame(
        [(1, 518, 1.000000000, 1, 10),
         (2, 520, 1.000000000, 1, 10),
         (4, 562, 6.000000000, 2, 10),
         (5, 682, 2.000000000, 2, 10),
         (6, 564, 8.000000000, 2, 10),                     
         (7, 565, 9.000000000, 2, 10)],
        ['id', 'productId', 'qtyOrdered', 'soId', 'typeId'],
    )

    expected_output = spark_session.createDataFrame(
        [(1, 518, 1, 10, float(1)),
         (2, 520, 1, 10, float(1)),
         (4, 562, 2, 10, float(6)),
         (5, 682, 2, 10, float(2)),
         (6, 564, 2, 10, float(8)),                     
         (7, 565, 2, 10, float(9))],
        ['id', 'productId', 'soId', 'typeId', 'qtyOrdered_r'],
    )

    # Actual output dataframe
    actual_output = d.round_qty_ordered(df_name, input)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method
    actual_output = actual_output.toPandas()
    expected_output = expected_output.toPandas()

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Transformation on DataFrame '{df_name}' completed: Column qtyOrdered was rounded to zero digits. Row count is 6" in caplog.text


def test_create_fact_sales(spark_session, caplog):

    caplog.set_level(logging.INFO)
    
    # Categorization paramenters
    id_15 = c.id_15
    id_14 = c.id_14
    id_13 = c.id_13
    id_12 = c.id_12
    id_11 = c.id_11
    id_10 = c.id_10
    id_9 = c.id_9
    id_8 = c.id_8
    id_7 = c.id_7
    id_6 = c.id_6
    id_5 = c.id_5
    id_4 = c.id_4
    id_3 = c.id_3
    id_2 = c.id_2
    id_1 = c.id_1
    
    # Input and expected output dataframes
    so_items_input = spark_session.createDataFrame(
        [(1, 518, 13784, 10, float(1)),
        (2, 520, 13784, 10, float(1)),
        (3, 562, 13787, 10, float(6)),
        (4, 682, 13787, 10, float(2)),
        (5, 564, 13787, 10, float(8)),
        (6, 300, 13787, 10, float(5)),
        (7, 301, 13787, 10, float(5)),
        (8, 302, 14601, 10, float(2)),
        (9, 303, 14601, 10, float(2)),                     
        (10, 304, 14622, 10, float(2))],
        ['id', 'productId', 'soId', 'typeId', 'qtyOrdered_r'],
    )

    so_input = spark_session.createDataFrame(
        [(13784, 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C'),         
        (13786, 1, id_14, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'G'),
        (13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H'),
        (14600, 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'J'),
        (14601, 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N'),
        (14602, 1, id_10, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'A'),
        (14603, 1, id_9, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'B'),
        (14604, 1, id_8, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'E'),
        (14605, 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'F'),
        (14606, 1, id_6, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'I'),
        (14607, 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'M'),
        (14608, 1, id_4, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'Q'),
        (14609, 1, id_3, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'R'),         
        (14614, 1, 3701, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'K'),
        (14615, 1, id_15, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'C'),
        (14616, 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 19, 20, 'N'),
        (14617, 1, 130, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'O'),
        (14618, 1, id_7, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'F'),
        (14619, 1, id_5, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'M'),
        (14620, 1, id_12, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 9, 20, 'J'),
        (14621, 1, 920, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 17, 20, 'P'),
        (14622, 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20, 'P')],
        ['id', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel'],
    )

    product_input = spark_session.createDataFrame(
        [(518, 2),
        (520, 3),
        (562, 4),
        (682, 5),
        (564, 6),
        (300, 7),
        (301, 8),
        (302, 9),
        (303, 10),                     
        (304, 11)],
        ['id', 'partId'],
    )

    part_input = spark_session.createDataFrame(
         [(2, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX4', 10.00, 22.00, 19.50),
         (3, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX5', 10.00, 22.00, 19.50),
         (4, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX6', 8.00, 24.00, 30.00),
         (5, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX7', 8.00, 24.00, 30.00),
         (6, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX8', 15.00, 15.00, 20.00),
         (7, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX9', 15.00, 15.00, 20.00),
         (8, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX10', 15.00, 15.00, 20.00),
         (9, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX11', 15.00, 15.00, 20.00),
         (10, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX12', 15.00, 15.00, 20.00),                    
         (11, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX13', 15.00, 15.00, 20.00)],
        ['id', 'typeId', 'customFields', 'masked_num', 'len_r', 'width_r', 'height_r'],
    )

    # COMPLETE JOINED TABLE
    # expected_output = spark_session.createDataFrame(
    #     [(1, 518, 13784, 10, float(1), 13784, 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C', 518, 2, 2, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX4', 10.00, 22.00, 19.50),
    #     (2, 520, 13784, 10, float(1), 13784, 1, id_15, "2021-01-05 00:00:00", "2021-01-01 00:00:00", 1, 10, 20, 'C', 520, 3, 3, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX5', 10.00, 22.00, 19.50),
    #     (3, 562, 13787, 10, float(6), 13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H', 562, 4, 4, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX6', 8.00, 24.00, 30.00),
    #     (4, 682, 13787, 10, float(2), 13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H', 682, 5, 5, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX7', 8.00, 24.00, 30.00),
    #     (5, 564, 13787, 10, float(8), 13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H', 564, 6, 6, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX8', 15.00, 15.00, 20.00),
    #     (6, 300, 13787, 10, float(5), 13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H', 300, 7, 7, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XXX9', 15.00, 15.00, 20.00),
    #     (7, 301, 13787, 10, float(5), 13787, 1, id_13, "2021-01-04 00:00:00", "2021-01-01 00:00:00", 1, 8, 20, 'H', 301, 8, 8, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX10', 15.00, 15.00, 20.00),
    #     (8, 302, 14601, 10, float(2), 14601, 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N', 302, 9, 9, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX11', 15.00, 15.00, 20.00),
    #     (9, 303, 14601, 10, float(2), 14601, 1, id_11, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 8, 20, 'N', 303, 10, 10, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX12', 15.00, 15.00, 20.00),                     
    #     (10, 304, 14622, 10, float(2), 14622, 1, 3125, "2021-01-05 00:00:00", "2021-01-04 00:00:00", 1, 12, 20, 'P', 304, 11, 11, 10, '{"1": {"name": "Country of Origin", "type": "Drop-Down List", "value": "Rwanda"}}', 'R_6XX13', 15.00, 15.00, 20.00)],
    #     ['id', 'productId', 'soId', 'typeId', 'qtyOrdered_r', 'id', 'currencyId', 'customerId', 'dateCompleted', 'dateCreated', 'locationGroupId', 'qbClassId', 'statusId', 'sales_channel', 'id', 'partId', 'id', 'typeId', 'customFields', 'masked_num', 'len_r', 'width_r', 'height_r'],
    # )

    expected_output = spark_session.createDataFrame(
        [("2021-01-01 00:00:00", 'C', 'R_6XXX4', float(1)),
        ("2021-01-01 00:00:00", 'C', 'R_6XXX5', float(1)),
        ("2021-01-01 00:00:00", 'H', 'R_6XXX6', float(6)),
        ("2021-01-01 00:00:00", 'H', 'R_6XXX7', float(2)),
        ("2021-01-01 00:00:00", 'H', 'R_6XXX8', float(8)),
        ("2021-01-01 00:00:00", 'H', 'R_6XXX9', float(5)),
        ("2021-01-01 00:00:00", 'H', 'R_6XX10', float(5)),
        ("2021-01-04 00:00:00", 'N', 'R_6XX11', float(2)),
        ("2021-01-04 00:00:00", 'N', 'R_6XX12', float(2)),                     
        ("2021-01-04 00:00:00", 'P', 'R_6XX13',  float(2))],
        ['Date_Id', 'Sales_Channel_Id', 'Product_Id', 'Units_Sold'],
    )

    # Actual output dataframe
    actual_output = d.create_fact_sales(so_items_input, so_input, product_input, part_input)
    
    # Convert Pyspark DataFrame to Pandas DataFrame to be able to use equality assertion via testing.assert_frame_equal method. 
    # In addtion, using get_sorted_data_frame function, to have the same row order for both DataFrames
  
    actual_output = get_sorted_data_frame(
        actual_output.toPandas(),
        ['Date_Id', 'Sales_Channel_Id', 'Product_Id', 'Units_Sold'],
    )
    
    expected_output = get_sorted_data_frame(
        expected_output.toPandas(),
        ['Date_Id', 'Sales_Channel_Id', 'Product_Id', 'Units_Sold'],
    )

    # Equality assertion
    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )

    assert f"Join completed. DataFrame 'soitem' joined with DataFrame 'so'. Row count is 10" in caplog.text
    assert f"Join completed. DataFrame 'categorized_items' joined with DataFrame 'product'. Row count is 10" in caplog.text
    assert f"Join completed. DataFrame 'categorized_items' joined with DataFrame 'part'. Row count is 10" in caplog.text
    assert f"Fact Sales table was created. Row count is 10" in caplog.text