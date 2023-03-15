from pyspark.sql import SparkSession
from ...src.my_sample_spark_app import read_register_spark_view


def assert_view_data_count(spark_session, view_name, expected_count):
    sql_text = f'select * from {view_name}'
    assert spark_session.sql(sql_text).count() == expected_count


class TestSpark:

    def test_generate_spark_context(self, get_spark_session):
        assert type(get_spark_session) == SparkSession
        get_spark_session.stop()

    def test_register_views(self, initialize_spark, request):
        spark_session, path_merchant_table, path_customer_table, path_transaction_table, \
        etl_date, output_dir = initialize_spark
        read_register_spark_view(spark_session=spark_session, view_name='v_merchant',
                                 file_path=path_merchant_table, csv_file=True)
        read_register_spark_view(spark_session=spark_session, view_name='v_customer',
                                 file_path=path_customer_table, csv_file=True)
        read_register_spark_view(spark_session=spark_session, view_name='v_transaction',
                                 file_path=path_transaction_table, csv_file=True)
        assert_view_data_count(spark_session=spark_session,
                               view_name='v_merchant',
                               expected_count=25)
        assert_view_data_count(spark_session=spark_session,
                               view_name='v_customer',
                               expected_count=187258)
        assert_view_data_count(spark_session=spark_session,
                               view_name='v_transaction',
                               expected_count=52459)
