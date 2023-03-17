from spark_app.com.thoughtworks.www.src.mercury import run_anonymize_customer_data
from spark_app.com.thoughtworks.www.src.utilities import read_data


class TestsIntegrationMercury:

    def test_it_run_anonymize_customer_data(self, initialize_spark):
        expected_result = 0
        expected_rows = 100
        expected_number_of_columns = 5
        spark_session, path_customer_data, path_customer_anonymize_data, etl_date = initialize_spark
        actual_result = run_anonymize_customer_data(spark_session=spark_session,
                                                    input_csv_path=path_customer_data,
                                                    output_path=path_customer_anonymize_data,
                                                    etl_date=etl_date)
        assert expected_result == actual_result
        actual_dataframe = read_data(spark_session=spark_session,
                                     file_path=path_customer_anonymize_data)
        assert expected_rows == actual_dataframe.count()
        assert expected_number_of_columns == len(actual_dataframe.columns)
