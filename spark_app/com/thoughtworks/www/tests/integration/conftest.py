from pathlib import Path

import pytest

from ...src.utilities import create_spark_context

GDP_ENVIRONMENT = 'test'
ETL_DATE = '16/03/2023'


@pytest.fixture(scope='session')
def initialize_spark(request):
    """Create a single node Spark application."""
    print("starting spark connection")
    base_dir = str(Path(__file__).parent / "test_data")
    output_dir = str(Path(__file__).parent.parent / "output")
    path_customer_data = base_dir + "/customer_pii"
    path_customer_anonymize_data = output_dir + "/customer_anonymize_pii"
    spark_session = create_spark_context(gdp_environment=GDP_ENVIRONMENT, etl_date=ETL_DATE)
    yield spark_session, path_customer_data, path_customer_anonymize_data, ETL_DATE
    print('tearing down')
    spark_session.stop()
