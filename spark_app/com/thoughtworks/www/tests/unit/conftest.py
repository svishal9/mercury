from pathlib import Path

import pytest

from ...src.utilities import create_spark_context, generate_dummy_customer_csv_file

GDP_ENVIRONMENT = 'test'
ETL_DATE = '16/03/2023'


@pytest.fixture
def get_spark_session():
    return create_spark_context(gdp_environment=GDP_ENVIRONMENT, etl_date=ETL_DATE)


@pytest.fixture(scope='session')
def initialize_spark(request):
    """Create a single node Spark application."""
    print("starting spark connection")
    base_dir = str(Path(__file__).parent / "test_data")
    output_dir = str(Path(__file__).parent.parent / "output")
    path_merchant_table = base_dir + "/retailer"
    path_customer_pii_table = base_dir + "/customer_pii"
    spark_session = create_spark_context(gdp_environment=GDP_ENVIRONMENT, etl_date=ETL_DATE)
    generate_dummy_customer_csv_file(path_customer_pii_table, spark_session, ETL_DATE)
    yield spark_session, path_merchant_table, ETL_DATE, output_dir
    print('tearing down')
    spark_session.stop()
