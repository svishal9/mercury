import datetime

from pyspark import Row
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from ...src.utilities import read_register_spark_view, anonymize_customer_information


def assert_view_data_count(spark_session, view_name, expected_count):
    sql_text = f'select * from {view_name}'
    assert spark_session.sql(sql_text).count() == expected_count


def assert_data_frames_are_equal_row_wise(data_frame_expected: DataFrame, data_frame_actual: DataFrame):
    assert (data_frame_expected.exceptAll(data_frame_actual).count() == 0)
    assert (data_frame_actual.exceptAll(data_frame_expected).count() == 0)


class TestSpark:

    def test_generate_spark_context(self, get_spark_session):
        assert type(get_spark_session) == SparkSession
        get_spark_session.stop()

    def test_register_views(self, initialize_spark, request):
        spark_session, path_merchant_table, \
        etl_date, output_dir = initialize_spark
        read_register_spark_view(spark_session=spark_session, view_name='v_merchant',
                                 file_path=path_merchant_table, csv_file=True)
        assert_view_data_count(spark_session=spark_session,
                               view_name='v_merchant',
                               expected_count=25)

    def test_anonymize_customer_information(self, initialize_spark):
        spark_session, _, _, _ = initialize_spark
        columns = ["firstName", "lastName", "address", "dateOfBirth"]
        data = [("John", "Doe", "3632 Roy Turnpike Angelamouth, MS 71759", datetime.date(1947, 2, 10)),
                ("Tracey", "Sommers", "42202 Rodriguez Road Tracyshire, TN 01262", datetime.date(2005, 11, 24)),
                ("Tracey", "Sommers", "42202 Rodriguez Road Tracyshire, TN 01262", datetime.date(2005, 11, 24)),
                ("Amy", "Singh", "3035 Henry Spur Apt. 089 Lake Christophershire, ND 74565", datetime.date(1990, 7, 3)),
                ("Amy", "Singh", "3035 Henry Spur Apt. 089 Lake Christophershire, ND 74565", datetime.date(1990, 7, 3)),
                ("Amy", "Singh", "3035 Henry Spur Apt. 089 Lake Christophershire, ND 74565", datetime.date(1990, 7, 3))]

        sort_column = ["maxRowNum"]
        sort_data = [1, 2, 3]

        df = spark_session.createDataFrame(data=data, schema=columns)
        df_anonymize_customer_information = anonymize_customer_information(df)
        df_anonymize_customer_information.createOrReplaceTempView("anonymizeCustomerInformation")
        assert_view_data_count(spark_session=spark_session,
                               view_name='anonymizeCustomerInformation',
                               expected_count=6)
        df_sort_actual = df_anonymize_customer_information.groupBy(columns).max("rowNumber") \
            .select(col('max(rowNumber)').alias("maxRowNum")).sort("maxRowNum")
        sort_rows = map(lambda x: Row(x), sort_data)
        df_sort_expected = spark_session.createDataFrame(data=sort_rows, schema=sort_column, ).sort("maxRowNum")
        assert_data_frames_are_equal_row_wise(data_frame_expected=df_sort_expected, data_frame_actual=df_sort_actual)
