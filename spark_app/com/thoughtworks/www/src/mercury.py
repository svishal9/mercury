import argparse
import logging
from datetime import datetime

__version__ = "0.1.0"

from ..src.utilities import logger, generate_dummy_customer_csv_file, \
    read_data, anonymize_customer_information, write_dataframe_to_csv, create_spark_context
from spark_app.com.thoughtworks.www.src.utils_logging import log_message

logger.setLevel(logging.INFO)


def run_anonymize_customer_data(spark_session, input_csv_path, output_path, etl_date):
    log_message("Generating csv file with dummy customer data (first_name, last_name, address and date_of_birth)")
    generate_dummy_customer_csv_file(path_customer_pii_table=input_csv_path,
                                     spark_session=spark_session,
                                     etl_date=etl_date)
    log_message("Reading customer csv data")
    df_customer_data = read_data(spark_session=spark_session,
                                 file_path=input_csv_path)
    log_message("Starting process to anonymize dummy customer data (first_name, last_name, address and date_of_birth)")
    df_anonymize_customer_information = anonymize_customer_information(df_input=df_customer_data)
    log_message(f"Writing anonymize customer data to csv at: {output_path}")
    write_dataframe_to_csv(input_dataframe=df_anonymize_customer_information,
                           output=output_path)
    return 0


def parse_arguments():
    parser = argparse.ArgumentParser(prog='mercury', description='Anonymize sensitive customer information')
    parser.add_argument('--customer-data-location', '-c', required=True, dest='customer_data_location',
                        help='Location of csv file containing sensitive customer data')
    parser.add_argument('--output-data-location', '-o', required=True, dest='output_data_location',
                        help='Location of csv file containing anonymous customer data')
    parser.add_argument('--environment', '-e', required=True, dest='gdp_environment',
                        help='Environment on which this program is executed. Example dev, test, Prod ..etc')
    log_message('Parsing parameters')
    args = parser.parse_args()
    return args.customer_data_location, args.output_data_location, args.gdp_environment


def main():
    log_message("Welcome to Mercury - tool to anonymize data.")
    etl_date = datetime.now().strftime("%d/%m/%Y")
    customer_data_location, output_data_location, gdp_environment = parse_arguments()
    spark_session = create_spark_context(gdp_environment=gdp_environment, etl_date=etl_date)
    run_anonymize_customer_data(spark_session=spark_session,
                                input_csv_path=customer_data_location,
                                output_path=output_data_location,
                                etl_date=etl_date)
    log_message("Operation successfully completed!")


if __name__ == '__main__':
    main()
