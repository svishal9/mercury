from pathlib import Path

import pytest
from ...src.my_sample_spark_app import create_spark_context


gdp_environment = 'local'
etl_date = '07/05/2021'


@pytest.fixture
def get_spark_session():
    return create_spark_context(gdp_environment=gdp_environment, etl_date=etl_date)


@pytest.fixture(scope='session')
def initialize_spark(request):
    """Create a single node Spark application."""
    print("starting spark connection")
    base_dir = str(Path(__file__).parent / "test_data")
    output_dir = str(Path(__file__).parent.parent / "output")
    path_merchant_table = base_dir + "/retailer"
    path_customer_table = base_dir + "/customer"
    path_transaction_table = base_dir + "/transaction"
    spark_session = create_spark_context(gdp_environment=gdp_environment, etl_date=etl_date)
    yield spark_session, path_merchant_table, path_customer_table, path_transaction_table, etl_date, output_dir
    print('tearing down')
    spark_session.stop()
